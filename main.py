#!/usr/bin/env python3

# most of this code came from "michael-danello/iMessageWrapped" but he didn't have a license so I made this
# MIT license since it's the best free for all.

import getpass
import os
import sqlite3
import re
import pandas as pd

ALL_TEXT = """
SELECT
    message.text,
    datetime (message.date / 1000000000 + strftime ("%s", "2001-01-01"), "unixepoch", "localtime") AS message_date,
    message.is_from_me,
    chat.chat_identifier as number
FROM
    message
JOIN chat_message_join cmj on cmj.message_id = message.ROWID
JOIN chat on chat.ROWID = cmj.chat_id
WHERE
    chat_identifier REGEXP '\+1\d{10}'
AND
    text not NULL
ORDER BY
    message_date DESC;
"""


ALL_TEXT_FROM_ME = """
SELECT
    message.text,
    datetime (message.date / 1000000000 + strftime ("%s", "2001-01-01"), "unixepoch", "localtime") AS message_date,
    message.is_from_me,
    chat.chat_identifier as number
FROM
    message
JOIN chat_message_join cmj on cmj.message_id = message.ROWID
JOIN chat on chat.ROWID = cmj.chat_id
WHERE
    chat_identifier REGEXP '\+1\d{10}'
AND
    text not NULL
AND
    message.is_from_me = 1
ORDER BY
    message_date DESC;
"""

ALL_CONTACTS = """
select record.ZFIRSTNAME as first_name, record.ZLASTNAME as last_name, numbers.ZFULLNUMBER as number
from ZABCDRECORD record
join ZABCDPHONENUMBER numbers on numbers.ZOWNER = record.Z_PK;
"""

def re_fn(expr, item):
    reg = re.compile(expr, re.I)
    return reg.search(item) is not None

def get_user():
    username = getpass.getuser()
    return username

def get_chat_db_path():
    username = get_user()
    return f"/Users/ptorelli/Library/Messages/chat.db"

def get_address_db_path():
    """
    iterate through address books and return all contact database paths
    """
    username = get_user()
    path = os.path.abspath(f"/Users/{username}/Library/Application Support/AddressBook/Sources")

    contact_db_paths = []
    # walking through the entire folder,
    # including subdirectories

    for folder, subfolders, files in os.walk(path):
        # checking the size of each file
        for file in files:
            if file == "AddressBook-v22.abcddb" and folder != path:
                contact_db_paths.append(os.path.join( folder, file))

    print(contact_db_paths)
    return contact_db_paths


def get_db(source):
    db = sqlite3.connect(source)
    db.create_function("REGEXP", 2, re_fn)
    return db

CHAT_DATABASE = get_chat_db_path()
CONTACTS_DATABSES = get_address_db_path()

def format_number(number):
    """standardizes numbers in contacts"""

    chars_to_remove = " ()-"

    for char in chars_to_remove:
        number = number.replace(char, "")
    if number[0:2] != '+1':
        number = '+1' + number

    return number


def format_contacts(contacts):
    """format firstname, lastname and number of contacts"""

    contacts['number'] = contacts['number'].apply(format_number)
    contacts = contacts.fillna("")
    contacts = contacts.set_index('number')
    contacts_dict = contacts.to_dict()
    full_name_contacts_dict = {}

    for key, value in contacts_dict['first_name'].items():
        full_name_contacts_dict[key] = value + " " + contacts_dict['last_name'][key]

    return full_name_contacts_dict


def get_contacts():
    """ A user may mantain multiple local contacts databases. Iterate through
    all of these databaes, extract all contacts from each and update the contact
    dict with results. Return the contacts dict"""

    contacts_dict = {}
    for contacts_db in CONTACTS_DATABSES:

        contacts_conn = get_db(contacts_db)
        contacts_cur = contacts_conn.cursor()
        all_contacts = pd.read_sql(ALL_CONTACTS, contacts_conn)
        contacts_dict.update(format_contacts(all_contacts))

    return contacts_dict

def get_text():
    chat_conn = get_db(CHAT_DATABASE)
    text_df = pd.read_sql(ALL_TEXT, chat_conn)
    return text_df

if __name__ == '__main__':

    contacts = get_contacts()
    text_df = get_text()
    number = text_df['number']
    text = text_df['text']
    dates = text_df['message_date']
    isme = text_df['is_from_me']
    for i in range(0, len(number)-1):
        if number[i] in contacts:
            ph = contacts[number[i]]
        else:
            # Original code broke if there was no contact
            ph = 'missing(' + number[i] + ')'
        print("%s %s - %s - %s" % (("to:" if isme[i] == 1 else "from:"), ph, dates[i], text[i]))
    #text = " ".join(text_df['text'])
    #return text


