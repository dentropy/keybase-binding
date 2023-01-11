from string import Template
import subprocess
import json
from database import DB, Messages, Users
from urlextract import URLExtract
import datetime
from sqlalchemy import distinct, desc

class ExportKeybase():
    def __init__(self):
        """Initialize the ExportKeybase object."""
        self.extractor = URLExtract()
    
    def get_teams(self):
        """Return string list of all current-user Keybase teams."""
        keybase_teams = subprocess.check_output(["keybase", "team", "list-memberships"])
        team_string = str(keybase_teams).split("\\n")
        teams = []
        for i in team_string[1:-1]:
            teams.append(i.split()[0])
        return teams

    def get_team_memberships(self, team_name):
        """Return string list of all users for a specific team."""
        json_string = '''
        {
            "method": "list-team-memberships",
            "params": {
                "options": {
                    "team": "%s"
                }
            }
        }
        ''' % (team_name)
        response = subprocess.check_output(["keybase", "team", "api", "-m", json_string])
        user_data = json.loads(response.decode('utf-8'))
        usernames = []
        for key in user_data["result"]["members"].keys():
            if user_data["result"]["members"][key] != None:
                for mah_val in range(len(user_data["result"]["members"][key])):
                    usernames.append(user_data["result"]["members"][key][mah_val]["username"])
        return usernames
    
    def get_user_metadata(self, username):
        """Get string of URLs for accounts that user has linked with Keybase account."""
        user_metadata = {"verification":[]}
        response = subprocess.check_output(["keybase", "id", username],stderr=subprocess.STDOUT, encoding="utf-8")
        response_string = str(response)[1:-1]#response.decode("utf-8")
        for line in response_string.split("\n"):
            if "admin of" in line:
                user_metadata["verification"].append(line.split()[6][5:-6])
        for url in self.extractor.find_urls(response_string):
            user_metadata["verification"].append(url)
        json_string = '''
        {
            "method": "list-user-memberships", 
            "params": {
                "options": {"username": "%s"}
            }
        }
        ''' % (username)
        response = json.loads(subprocess.check_output(["keybase", "team", "api", "-m", json_string]).decode('utf-8'))
        team_list = []
        for team in response["result"]["teams"]:
            team_list.append(team["fq_name"])
        user_metadata["teams"] = team_list
        user_metadata["followers"] = subprocess.check_output(
            ["keybase", "list-followers", username],stderr=subprocess.STDOUT, encoding="utf-8").split("\n")
        user_metadata["following"] = subprocess.check_output(
            ["keybase", "list-following", username],stderr=subprocess.STDOUT, encoding="utf-8").split("\n")
        return user_metadata

    
    def get_team_channels(self,keybase_team_name):
        """Returns list of strings for each text channel on a team."""
        get_teams_channels = Template('''
        {
            "method": "listconvsonname",
            "params": {
                "options": {
                    "topic_type": "CHAT",
                    "members_type": "team",
                    "name": "$TEAM_NAME"
                }
            }
        }
        ''')
        dentropydaemon_channels_json = get_teams_channels.substitute(TEAM_NAME=keybase_team_name)
        dentropydaemon_channels = subprocess.check_output(["keybase", "chat", "api", "-m", dentropydaemon_channels_json])
        dentropydaemon_channels = str(dentropydaemon_channels)[2:-3]
        mah_json = json.loads(dentropydaemon_channels)
        mah_channels = []
        for i in mah_json["result"]["conversations"]:
            mah_channels.append(i["channel"]["topic_name"])
        return mah_channels

    def get_team_chat_channel(self, keybase_team_name, keybase_topic_name):
        """Returns json object of all messages within a Keybase team topic"""
        get_teams_channels = Template('''
        {
            "method": "read",
            "params": {
                "options": {
                    "channel": {
                        "name": "$TEAM_NAME",
                        "members_type": "team",
                        "topic_name": "$TOPIC_NAME"
                    }
                }
            }
        }
        ''')
        dentropydaemon_channels_json = get_teams_channels.substitute(TEAM_NAME=keybase_team_name, TOPIC_NAME=keybase_topic_name)
        command = ["keybase", "chat", "api", "-m", dentropydaemon_channels_json]
        response = subprocess.check_output(command)
        return json.loads(response.decode('utf-8'))
 
    def get_latest_topic_message(self, keybase_team_name, keybase_topic_name):
        get_teams_channels = Template('''{
            "method": "read",
                "params": {
                    "options": {
                        "channel": {
                            "name": "$TEAM_NAME",
                            "members_type": "team",
                            "topic_name": "$TOPIC_NAME"
                        },
                        "pagination": {
                            "num": 1
                        }
                    }
                }
            }
            ''')
        dentropydaemon_channels_json = get_teams_channels.substitute(
            TEAM_NAME=keybase_team_name, 
            TOPIC_NAME=keybase_topic_name
        )
        command = ["keybase", "chat", "api", "-m", dentropydaemon_channels_json]
        response = subprocess.check_output(command)
        return json.loads(response.decode('utf-8'))

    def get_topic_messages_without_pagination(self, keybase_team_name, keybase_topic_name):
        get_teams_channels = Template('''{
            "method": "read",
                "params": {
                    "options": {
                        "channel": {
                            "name": "$TEAM_NAME",
                            "members_type": "team",
                            "topic_name": "$TOPIC_NAME"
                        },
                        "pagination": {
                            "num": 100
                        }
                    }
                }
            }
            ''')
        dentropydaemon_channels_json = get_teams_channels.substitute(
            TEAM_NAME=keybase_team_name, 
            TOPIC_NAME=keybase_topic_name
        )
        command = ["keybase", "chat", "api", "-m", dentropydaemon_channels_json]
        response = subprocess.check_output(command)
        return json.loads(response.decode('utf-8'))
    
    def get_topic_messages_with_pagination(self, keybase_team_name, keybase_topic_name, PAGIATION):
        get_teams_channels = Template('''{
        "method": "read",
            "params": {
                "options": {
                    "channel": {
                        "name": "$TEAM_NAME",
                        "members_type": "team",
                        "topic_name": "$TOPIC_NAME"
                    },
                    "pagination": {
                        "next": "$PAGIATION",
                        "num": 100
                    }
                }
            }
        }
        ''')
        dentropydaemon_channels_json = get_teams_channels.substitute(
            TEAM_NAME=keybase_team_name, 
            TOPIC_NAME=keybase_topic_name,
            PAGIATION = PAGIATION
        )
        command = ["keybase", "chat", "api", "-m", dentropydaemon_channels_json]
        response = subprocess.check_output(command)
        return json.loads(response.decode('utf-8'))
    
    def get_until_topic_id(self, team_name, team_topic, min_topic_id):
        previous_query = self.get_topic_messages_without_pagination(team_name, team_topic)
        current_msg_id = previous_query["result"]["messages"][0]["msg"]["id"]
        mah_messages = previous_query
        for i in range(current_msg_id - int(previous_query["result"]["messages"][0]["msg"]["id"] / 100) ):
            if "next" in previous_query["result"]["pagination"]:
                more_messages = self.get_topic_messages_with_pagination(team_name, 
                    team_topic, previous_query["result"]["pagination"]["next"])
                for message in more_messages["result"]["messages"]:
                    mah_messages["result"]["messages"].append(message)
                previous_query = more_messages
        delete_entries = []
        for message in range(len(mah_messages["result"]["messages"])) :
            if mah_messages["result"]["messages"][message]["msg"]["id"] < min_topic_id:
                delete_entries.append(message)
        delete_entries.reverse()
        for message_id in delete_entries:
            del mah_messages["result"]["messages"][message_id]
        return mah_messages
    
    def export_team_user_metadata_sql(self, team_name, sql_connection_string):
        """Write to sql database all users and metadata for a given team."""
        db = DB(sql_connection_string)
        member_list = self.get_team_memberships(team_name)
        members = {}
        for member in member_list:
            print("Getting " + member + "'s metadata")
            user_metadata = self.get_user_metadata(member)
            db.session.add( Users( 
                username = member, 
                teams = json.dumps(user_metadata["teams"]), 
                verification = json.dumps(user_metadata["verification"]), 
                followers = json.dumps(user_metadata["followers"]), 
                following =  json.dumps(user_metadata["following"]),
            ))
            members[member] = user_metadata
        db.session.commit()
        return members
     
    def get_root_messages(self, mah_messages, db):
        """From message list, find text messages, add them to SQL database session, and then commit the session."""
        for message in mah_messages["result"]["messages"]:
            if message["msg"]["content"]["type"] == "headline":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "headline",
                    txt_body =  message["msg"]["content"]["headline"]["headline"],
                    from_user = message["msg"]["sender"]["username"],
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                    ))
            elif message["msg"]["content"]["type"] == "join":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "join",
                    from_user = message["msg"]["sender"]["username"],
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                    ))
            elif message["msg"]["content"]["type"] == "metadata":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "metadata",
                    from_user = message["msg"]["sender"]["username"],
                    txt_body =  json.dumps(message["msg"]["content"]["metadata"]),
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"])
                    ))
            elif message["msg"]["content"]["type"] == "attachment":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "attachment",
                    from_user = message["msg"]["sender"]["username"],
                    txt_body =  json.dumps(message["msg"]["content"]["attachment"]),
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"])
                    ))
            elif message["msg"]["content"]["type"] == "unfurl":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "unfurl",
                    from_user = message["msg"]["sender"]["username"],
                    txt_body =  json.dumps(message["msg"]["content"]["unfurl"]),
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"])
                    ))
            elif message["msg"]["content"]["type"] == "system":
                if "at_mention_usernames" in message["msg"]:
                    at_mention_usernames = json.dumps(message["msg"]["at_mention_usernames"])
                else:
                    at_mention_usernames = None
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "system",
                    from_user = message["msg"]["sender"]["username"],
                    txt_body =  json.dumps(message["msg"]["content"]["system"]),
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                    userMentions = at_mention_usernames
                    ))
            elif message["msg"]["content"]["type"] == "leave":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "leave",
                    from_user = message["msg"]["sender"]["username"],
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                    ))
            elif message["msg"]["content"]["type"] == "delete":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "delete",
                    from_user = message["msg"]["sender"]["username"],
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                    msg_reference = message["msg"]["content"]["delete"]["messageIDs"][0]
                    ))
            elif message["msg"]["content"]["type"] == "reaction":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "reaction",
                    from_user = message["msg"]["sender"]["username"],
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                    reaction_body =  message["msg"]["content"]["reaction"]["b"],
                    msg_reference = message["msg"]["content"]["reaction"]["m"]
                ))
            elif message["msg"]["content"]["type"] == "edit":
                db.session.add( Messages( 
                    team = message["msg"]["channel"]["name"], 
                    topic = message["msg"]["channel"]["topic_name"],
                    msg_id = message["msg"]["id"],
                    msg_type = "edit",
                    txt_body =  message["msg"]["content"]["edit"]["body"],
                    from_user = message["msg"]["sender"]["username"],
                    sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                    msg_reference = message["msg"]["content"]["edit"]["messageID"]
                ))
            elif message["msg"]["content"]["type"] == "text":
                urls = self.extractor.find_urls(message["msg"]["content"]["text"]["body"])
                if len(urls) == 0:
                    db.session.add( Messages( 
                        team = message["msg"]["channel"]["name"], 
                        topic = message["msg"]["channel"]["topic_name"],
                        msg_id = message["msg"]["id"],
                        msg_type = "text",
                        from_user = message["msg"]["sender"]["username"],
                        sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                        txt_body =  message["msg"]["content"]["text"]["body"],
                        word_count = len(message["msg"]["content"]["text"]["body"].split(" ")),
                        userMentions = json.dumps(message["msg"]["content"]["text"]["userMentions"])
                        ))
                else:
                    db.session.add( Messages( 
                        team = message["msg"]["channel"]["name"], 
                        topic = message["msg"]["channel"]["topic_name"],
                        msg_id = message["msg"]["id"],
                        msg_type = "text",
                        from_user = message["msg"]["sender"]["username"],
                        sent_time = datetime.datetime.utcfromtimestamp(message["msg"]["sent_at"]),
                        txt_body =  message["msg"]["content"]["text"]["body"],
                        urls = json.dumps(urls),
                        num_urls = len(urls),
                        word_count = len(message["msg"]["content"]["text"]["body"].split(" ")),
                        userMentions = json.dumps(message["msg"]["content"]["text"]["userMentions"])
                        ))
        db.session.commit()

    def generate_sql_export(self, keybase_team, sql_connection_string):
        """Export keybase team topic messages strait from keybase to an SQL database"""
        keybase_teams = self.get_team_channels(keybase_team)
        db = DB(sql_connection_string)
        mah_messages = {"topic_name":{}}
        for topic_name in keybase_teams:
            mah_messages = self.get_until_topic_id(keybase_team, topic_name, 0)
            self.get_root_messages(mah_messages,db)
        print("Conversion from json to sql complete")

        
    # TODO rewrite to export from database
    def generate_json_export(self, keybase_team, output_file):
        """Creates a json file with specified filename containing all team chat data."""
        complexity_weekend_teams = self.get_team_channels(keybase_team)
        mah_messages = {"topic_name":{}}
        for topic in complexity_weekend_teams:
            #try:
            result_msgs = self.get_team_chat_channel(keybase_team, topic)
            print(result_msgs)
            result_msgs["result"]["messages"].reverse()
            mah_messages["topic_name"][topic] = result_msgs
            #except:
            #    print("Got an error")
        text_file = open(output_file, "w")
        text_file.write(json.dumps(mah_messages))
        text_file.close()

    def export_text_msgs_to_csv(self, sql_connection_string, output_file):
        """Export text messages from SQL database to CSV spreadsheet."""
        db = DB(sql_connection_string)
        mah_messages = db.session.query(Messages).filter_by(msg_type = "text")
        msg_list = [["text_messages"]]
        for message in mah_messages:
            msg_list.append([str(message.txt_body)])
        import csv
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(msg_list)

    def message_table_to_csv(self, table_object, sql_connection_string, csv_file_name):
        """Export table object with text message data to CSV spreadsheet."""
        db = DB(sql_connection_string)
        mah_columns = []
        for column in table_object.__table__.c:
            mah_columns.append(str(column).split(".")[1])
        import csv
        with open(csv_file_name, 'w') as f:
            out = csv.writer(f)
            out.writerow(mah_columns)
            for row in db.session.query(table_object).all():
                full_row = []
                for column_name in mah_columns:
                    full_row.append(row.__dict__[column_name])
                out.writerow(full_row)
        

    def sync_team_topics(self, keybase_team, sql_connection_string):
        keybase_teams = self.get_team_channels(keybase_team)
        db = DB(sql_connection_string)
        get_db_topics = db.session.query(distinct(Messages.topic)).filter_by(team=keybase_team)
        db_topic_list = []
        for topic_name in get_db_topics:
            db_topic_list.append(topic_name[0])
        print(db_topic_list)
        missing_topics = []
        for topic_name in keybase_teams:
            if topic_name not in db_topic_list:
                missing_topics.append(topic_name)
        if len(missing_topics) != 0:
            print("Looks like we have a problem")
        mah_missing_messages = {}
        for topic_name in db_topic_list:
            print("topic_name")
            print(topic_name)
            max_db_topic_id = db.session.query(Messages)\
            .filter_by(team=keybase_team)\
            .filter_by(topic=topic_name)\
            .order_by(desc(Messages.msg_id)).limit(1)
            max_db_topic_id = max_db_topic_id[0].msg_id
            most_recent_message = self.get_latest_topic_message(keybase_team, topic_name)
            most_recent_message_msg_id = most_recent_message["result"]["messages"][0]["msg"]["id"]
            if max_db_topic_id != most_recent_message_msg_id:
                missing_messages = self.get_until_topic_id(keybase_team, topic_name, max_db_topic_id + 1)
                mah_missing_messages[topic_name] = missing_messages
                self.get_root_messages(missing_messages,db)
        return mah_missing_messages

    def get_personal_chats(self):
        # keybase chat api -m '{"method": "list"}'
        response = subprocess.check_output(["keybase", "chat", "api", "-m" ,'{"method": "list"}'])
        user_data = json.loads(response.decode('utf-8'))
        return user_data

    def export_personal_chat(self):
        pass

    def export_file_from_chat():
        pass

    def get_git_repos():
        # keybase git list
        pass 