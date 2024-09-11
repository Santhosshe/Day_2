import requests
import psycopg2
import time
import logging
import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(filename='discord_log_file.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DISCORD_AUTH_TOKEN=os.getenv("DISCORD_AUTH_TOKEN")
DISCORD_CHANNEL_ID_LIST=[1281499626742222870,1281499568630136842]
MESSAGE_SCANNING_INTERVAL=2

DISCORD_HEADERS={
    "Authorization":DISCORD_AUTH_TOKEN
}

## DB Credentials:
host=os.getenv("host")
password=os.getenv("password")
user=os.getenv("user")
database=os.getenv("database")
port=os.getenv("port")
    

Available_replies={
    'hi':'hello',
    'whats your name':'iam a bot i dont have any specific name',
    'how are you':'fine', 
    'how is your day':'it is great how about you',
    'hi this is santhosshe':'hello this is discord bot',
    'who is our current prime minister':'Our current Prime Mininster is Mr. Narendra Modi'
}

def creating_db_connection():
    """Here we are establishing connection to the PostgreSQL database 

    Returns:
        connection : connection object if connected successfully
        none : if not connection fails
    """
    try:
        conn=psycopg2.connect(
        host=host,
    password=password,
    user=user,
    database=database,
    port=port
    )
        logging.info("Connected to database successfully")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

def creating_table(conn):
    """Creates the 'Message_list' table in the database if it doesn't already exist.

    The table contains columns for storing message ID, global name, message content,
    timestamp, attachment ID, message type, and image URL.

    Args:
        conn (psycopg2.connection): The database connection object.

    Returns:
        bool: Returns True if the table is created successfully.
        None: If there's an error, returns None and prints the error message.
    """
    cur = conn.cursor()
    try:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Message_list (
                msg_id BIGINT PRIMARY KEY,
                Global_name TEXT,
                message TEXT,
                Msg_time TIMESTAMPTZ,
                Attach_id BIGINT,
                Msg_Type VARCHAR(50),
                Img_url VARCHAR(255)
            );
        ''')
        conn.commit()
        cur.close()
        logging.info("Created table successfully")
        return True
    except Exception as e:
        logging.error(f"Error creating the table: {e}")
        return None

def db_message_insert(conn,msg_id:int, recent_message:str, global_name:str, timestamp,attach_id:int,attach_type:str,url:str):
    """Inserts a new message record into the 'Message_list' table.

    If a message with the given ID already exists, it prints a message and skips insertion.

    Args:
        conn (psycopg2.connection): The database connection object.
        msg_id (int): The unique ID of the message.
        recent_message (str): The content of the message.
        global_name (str): The global name of the message author.
        timestamp (str): The time the message was sent.
        attach_id (int): The ID of any attachment associated with the message.
        attach_type (str): The type of the attachment (e.g., 'Image', 'Pdf').
        url (str): The URL of the attachment, if any.

    Returns:
        None
    """
    try:
        cur = conn.cursor()
        cur.execute('''select exists(select 1 from Message_list where msg_id=%s)''',(msg_id,))
        message_id_present=cur.fetchone()[0]
        if not message_id_present:
            cur.execute('''
           insert into Message_list (msg_id, Global_name, message, Msg_time,Attach_id,Msg_Type,Img_url) 
          values (%s, %s, %s, %s,%s,%s,%s);''', 
          (msg_id, global_name, recent_message, timestamp,attach_id,attach_type,url))  
            logging.info(f"Inserted message ID {msg_id} into the table")
        else:
            logging.info("Message id already exists No new Message")
        conn.commit()
        cur.close()
    except Exception as e:
        logging.error(f"Error inserting to the table: {e}")


def db_message_fetch(conn):
    """Fetches all messages from the 'Message_list' table.

    Args:
        conn (psycopg2.connection): The database connection object.

    Returns:
        list: A list of tuples, each representing a message record from the database.
        None: If there's an error, returns None and prints the error message.
    """
    try:
        cur = conn.cursor()
        cur.execute('''select * from Message_list;''' )
        messages=cur.fetchall()
        cur.close()
        logging.info("Fetched all messages from database")
        return messages
    except Exception as e:
        logging.error(f"Error fetching messages from the database: {e}")
        return None



def retrive_recent_messages_by_channel_id(channel_id): 
    """Fetches the most recent message from the specified Discord channel.

    This function retrieves the message content, message ID, author's global name, 
    timestamp, and attachment information (if any).

    Args:
        channel_id (int): The ID of the Discord channel.

    Returns:
        list: A list containing the message ID, content, author's global name, timestamp, 
              attachment ID, attachment type, and URL.
    """
    try:
        response=requests.get(f"https://discord.com/api/v9/channels/{channel_id}/messages", headers=DISCORD_HEADERS)
        messages_Info=response.json()
        
        if response.status_code == 200:
            logging.info(f"Fetched messages from channel ID {channel_id}.")
        else:
            logging.error(f"Failed to fetch messages from channel ID {channel_id}. Status code: {response.status_code}")
            
        Recent_message_info=messages_Info[0]
    
        content=Recent_message_info['content']
        recent_msg_id=Recent_message_info['id']
        name=Recent_message_info['author']['global_name']
        timestamp=Recent_message_info['timestamp']
        attach_type='Message'
        url='No attachment URL'
        attach_id=0

        if Recent_message_info['attachments']:
            attach_info=Recent_message_info['attachments']
            attach_id = Recent_message_info['attachments'][0]['id']

            if content=="" and  attach_info[0]['content_type']=='image/png':
                attach_type = 'Image'
                url=attach_info[0]['url']
            elif attach_info[0]['content_type']=='image/png':
                attach_type = 'Message/Image'
                url=attach_info[0]['url']
            elif content=="" and  attach_info[0]['content_type']=='application/pdf':
                attach_type = 'Pdf'
                url=attach_info[0]['url']

        RECENT_MESSAGE_LIST=[recent_msg_id,content,name,timestamp,attach_id,attach_type,url]
        return RECENT_MESSAGE_LIST
    except Exception as e:
        logging.error(f"Cannot Retrive message from channel ID{channel_id}")


def sending_messages_by_channel_id(channel_id,message_to_be_sent): 
    """Sends a message to the specified Discord channel.

    Args:
        channel_id (int): The ID of the Discord channel where the message should be sent.
        message_to_be_sent (str): The content of the message to be sent.

    Returns:
        bool: Returns True if the message was sent successfully, False otherwise.
    """
    try:
        payload={
        "content":message_to_be_sent
        }
        url=f"https://discord.com/api/v9/channels/{channel_id}/messages"
        sending_message=requests.post(url,payload,headers=DISCORD_HEADERS)
        logging.info(f"Message sent to channel ID{channel_id} successfully")
        return sending_message.status_code == 200
    
    except Exception as e:
        logging.error(f"Cannot send message to channel ID{channel_id}")

def main(channel_id,conn):
    """Recieves the recent message and insert it into the database then respond to the message if it is available in the existing replies

    Args:
        channel_id (int): The ID of the Discord channel from where the message should be recieved and sent 
        conn (psycopg2.connection): The database connection object

    Returns:
        bool: return true if message is recieved and sent successfully
        bool: return false if message is neither recieved nor sent successfully
    """
    recent_message=retrive_recent_messages_by_channel_id(channel_id)
    
    db_message_insert(conn,*recent_message)
    
    replying_message_content = recent_message[1].lower().strip()

    if replying_message_content in Available_replies:
        reply_to_be_sent = Available_replies[replying_message_content]
        sending_messages_by_channel_id(channel_id=channel_id, message_to_be_sent=reply_to_be_sent)
    
    return True
   

if __name__ == "__main__":
    
    conn = creating_db_connection()
    if not conn:
        logging.error("Database connection failed. Exiting.")
        exit(1)
    table_creation=creating_table(conn)

    should_continue = True

    while should_continue:
        for channel_id in DISCORD_CHANNEL_ID_LIST:
            should_continue = main(channel_id, conn)
        time.sleep(MESSAGE_SCANNING_INTERVAL)

    conn.close()