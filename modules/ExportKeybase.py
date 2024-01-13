from string import Template
import subprocess
import json
from glob import glob
import os
from pprint import pprint
# from database import DB, Messages, Users
# from urlextract import URLExtract
import datetime
from pathlib import Path
import sqlite3
# from sqlalchemy import distinct, desc

class ExportKeybase():
    def __init__(self, save_dir="./out/"):
        """Initialize the ExportKeybase object."""
        # self.extractor = URLExtract()
        # Make folders if they do not exist
        self.save_dir = save_dir
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        Path(f"{self.save_dir}/Attachments").mkdir(parents=True, exist_ok=True)
        Path(f"{self.save_dir}/GitRepos").mkdir(parents=True, exist_ok=True)
        paths_in_dir = glob(self.save_dir + "/*")
        files_in_dir = []
        for path in paths_in_dir:
            files_in_dir.append( path.split("/")[-1] )
        self.con = sqlite3.connect(f"{self.save_dir}/keybase_export.sqlite")
        self.cur = self.con.cursor()
        self.cur.execute("CREATE TABLE IF NOT EXISTS whoami_t(keybase_username)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS teams_t(team_name)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS team_members_t(team_name, member_name)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS team_channels_t(team_name, channel_name)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS group_channels_t(group_name)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS group_messages_t(group_name, message_json)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS team_messages_t(team_name, topic_name, message_json)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS git_repos_t(git_repo_name, git_repo_path)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS followers_t(keybase_username)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS following_t(keybase_username)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS user_metadata_t(keybase_username, metadata)")
        self.con.commit()
        self.whoami = None
        self.teams = None
        self.team_members = None
        self.team_channels = None
        self.group_chats = None


    def get_keybase_username(self):
        if self.whoami == None:
            self.whoami = str(subprocess.check_output(["keybase", "whoami"]))[2:-3]
        return self.whoami

    def save_keybase_username(self):
        res = self.cur.execute("SELECT COUNT(*) FROM whoami_t").fetchone()[0]
        if res == 0:
            self.get_keybase_username()
            self.cur.execute(f"INSERT INTO whoami_t (keybase_username) VALUES ('{self.whoami}')")
            self.con.commit()
            return True
        else:
            self.whoami = self.cur.execute("SELECT keybase_username FROM whoami_t ").fetchone()[0]
            return False


    def get_teams(self):
        """Return string list of all current-user Keybase teams."""
        if self.teams == None:
            keybase_teams = subprocess.check_output(["keybase", "team", "list-memberships"])
            team_string = str(keybase_teams).split("\\n")
            self.teams = []
            for i in team_string[1:-1]:
                self.teams.append(i.split()[0])
            return self.teams
        else:
            return self.teams


    def save_teams(self):
        res = self.cur.execute("SELECT COUNT(*) FROM teams_t").fetchone()[0]
        if res != 0:
            return False
        formatted_teams = []
        for team in self.get_teams():
            formatted_teams.append((team,))
        self.cur.executemany("INSERT INTO teams_t(team_name) VALUES(?)", formatted_teams)
        self.con.commit()
        return True


    def get_members_of_team(self, team_name):
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


    def get_members_of_all_teams(self):
        if self.teams == None:
            self.get_teams()
        if self.team_members != None:
            return self.team_members
        else:
            self.team_members = {}
            for team in self.get_teams():
                print(f"Getting Members for Team {team}")
                self.team_members[team] = self.get_members_of_team(team)
            return self.team_members
                
    def save_team_members(self, team_name):
        team_members = self.get_members_of_team(team_name)
        formatted_team_member_list = []
        for team_member in team_members:
            formatted_team_member_list.append((team_name,team_member,))
        self.cur.executemany("INSERT INTO team_members_t(team_name, member_name) VALUES(?, ?)", formatted_team_member_list)
        self.con.commit()
        return True


    def save_members_of_all_teams(self):
        res = self.cur.execute("SELECT COUNT(*) FROM team_members_t").fetchone()[0]
        if res != 0:
            return False
        self.get_members_of_all_teams()
        formatted_team_member_list = []
        for team in self.team_members:
            for team_member in self.team_members[team]:
               formatted_team_member_list.append((team,team_member,))
        self.cur.executemany("INSERT INTO team_members_t(team_name, member_name) VALUES(?, ?)", formatted_team_member_list)
        self.con.commit()
        return True


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
            if "topic_name" in i["channel"]:
                mah_channels.append(i["channel"]["topic_name"])
        return mah_channels

    def save_team_channels(self, team_name):
        team_channels = self.get_team_channels(team_name)
        formatted_team_member_list = []
        for channel in team_channels:
            for team_member in team_channels:
               formatted_team_member_list.append((channel,team_member,))
        self.cur.executemany("INSERT INTO team_channels_t(team_name, channel_name) VALUES(?, ?)", formatted_team_member_list)
        self.con.commit()
        return True

    def get_all_team_channels(self):
        if self.teams == None:
            self.get_teams()
        if self.team_channels != None:
            return self.team_channels
        else:
            self.team_channels = {}
            for team in self.teams:
                print(f"Getting Channels for Team {team}")
                self.team_channels[team] = self.get_team_channels(team)
            return self.team_channels
        return self.team_channels


    def save_all_team_channels(self):
        res = self.cur.execute("SELECT COUNT(*) FROM team_channels_t").fetchone()[0]
        if res != 0:
            return False
        self.get_all_team_channels()
        formatted_team_member_list = []
        for channel in self.team_channels:
            for team_member in self.team_channels[channel]:
               formatted_team_member_list.append((channel,team_member,))
        self.cur.executemany("INSERT INTO team_channels_t(team_name, channel_name) VALUES(?, ?)", formatted_team_member_list)
        self.con.commit()
        return True


    def get_list_group_chats(self):
        if self.group_chats != None:
            return self.group_chats
        json_string = '''
        {
            "method": "list"
        }
        '''
        response = subprocess.check_output(["keybase", "chat", "api", "-m", json_string])
        self.group_chats = json.loads(response.decode('utf-8'))["result"]["conversations"]
        return self.group_chats


    def save_list_group_chats(self):
        self.get_list_group_chats()
        res = self.cur.execute("SELECT COUNT(*) FROM group_channels_t").fetchone()[0]
        if res != 0:
            return False
        sql_insert_list = []
        for channel in self.group_chats:
            sql_insert_list.append((json.dumps(channel),))
        self.cur.executemany("INSERT INTO group_channels_t(group_name) VALUES(json(?))", sql_insert_list)
        self.con.commit()
        return True



    def get_all_chat_messages_from_group(self, chat_name):
        another_page_exists = True 
        messages = []
        json_string = '''
        {
            "method": "read", 
            "params": {
                "options": {
                    "channel": {
                        "name": "%s"
                    }, 
                    "pagination": {
                        "num": 1000
                    }
                }
            }
        }
        '''  % (chat_name, chat_page, next_page_arg)
        response = subprocess.check_output(["keybase", "chat", "api", "-m", json_string])
        response_json =  json.loads(response.decode('utf-8'))
        another_page_exists = "last" not in response_json["result"]["pagination"].keys()
        while(another_page_exists):
            json_string = '''
            {
                "method": "read", 
                "params": {
                    "options": {
                        "channel": {
                            "name": "%s"
                        }, 
                        "pagination": {
                            "num": %d,
                            "next": "%s"
                        }
                    }
                }
            }
            '''  % (chat_name, chat_page, next_page_arg)
            response = subprocess.check_output(["keybase", "chat", "api", "-m", json_string])
            response_json =  json.loads(response.decode('utf-8'))
            messages += response_json["result"]["messages"]
            next_page_arg = response_json["result"]["pagination"]["next"]
            another_page_exists = "last" not in response_json["result"]["pagination"].keys()
        return messages


    def save_all_messages_from_channel(self, chat_name, folder_sub_path):
        res = self.cur.execute(f"SELECT COUNT(*) FROM group_messages_t WHERE group_name='{chat_name}'").fetchone()[0]
        if res != 0:
            return True
        dm_save_dir = f"{self.save_dir}/{folder_sub_path}"
        Path(dm_save_dir).mkdir(parents=True, exist_ok=True)
        another_page_exists = True 
        page_count = 1
        messages = []
        json_string = '''
        {
            "method": "read", 
            "params": {
                "options": {
                    "members_type": "team",
                    "channel": {
                        "name": "%s"
                    }, 
                    "pagination": {
                        "num": 1000
                    }
                }
            }
        }
        '''  % (chat_name)
        response = subprocess.check_output(["keybase", "chat", "api", "-m", json_string])
        response_json =  json.loads(response.decode('utf-8'))
        messages += response_json["result"]["messages"]
        another_page_exists = "last" not in response_json["result"]["pagination"].keys()
        while(another_page_exists):
            next_page_arg = response_json["result"]["pagination"]["next"]
            json_string = '''
            {
                "method": "read", 
                "params": {
                    "options": {
                        "channel": {
                            "name": "%s"
                        }, 
                        "pagination": {
                            "num": %d,
                            "next": "%s"
                        }
                    }
                }
            }
            '''  % (chat_name, 1000, next_page_arg)
            response = subprocess.check_output(["keybase", "chat", "api", "-m", json_string])
            response_json =  json.loads(response.decode('utf-8'))
            messages += response_json["result"]["messages"]
            another_page_exists = "last" not in response_json["result"]["pagination"].keys()
            if len(messages) >= 10000:
                json.dump(messages, open(f"{dm_save_dir}/{str(page_count).zfill(3)}.json", 'w'))
                sql_insert_list = []
                for message in messages:
                    sql_insert_list.append((chat_name,json.dumps(message),))
                self.cur.executemany("INSERT INTO group_messages_t(group_name, message_json) VALUES(?, json(?))", sql_insert_list)
                self.con.commit()
                messages = []
                page_count += 1
        json.dump(messages, open(f"{dm_save_dir}/{str(page_count).zfill(3)}.json", 'w'))
        sql_insert_list = []
        for message in messages:
               sql_insert_list.append((chat_name,json.dumps(message),))
        self.cur.executemany("INSERT INTO group_messages_t(group_name, message_json) VALUES(?, json(?))", sql_insert_list)
        self.con.commit()
        return True

    def save_all_messages_from_team_channel(self, team_name, topic_name, folder_sub_path):
        res = self.cur.execute(f"SELECT COUNT(*) FROM team_messages_t WHERE team_name='{team_name}' AND topic_name='{topic_name}'").fetchone()[0]
        if res != 0:
            return True
        dm_save_dir = f"{self.save_dir}/{folder_sub_path}"
        Path(dm_save_dir).mkdir(parents=True, exist_ok=True)
        another_page_exists = True 
        page_count = 1
        messages = []
        json_string = '''
        {
            "method": "read", 
            "params": {
                "options": {
                    "members_type": "team",
                    "channel": {
                        "name": "%s",
                        "members_type": "team",
                        "topic_name": "%s"
                    }, 
                    "pagination": {
                        "num": 1000
                    }
                }
            }
        }
        '''  % (team_name, topic_name)
        response = subprocess.check_output(["keybase", "chat", "api", "-m", json_string])
        response_json =  json.loads(response.decode('utf-8'))
        messages += response_json["result"]["messages"]
        another_page_exists = "last" not in response_json["result"]["pagination"].keys()
        while(another_page_exists):
            next_page_arg = response_json["result"]["pagination"]["next"]
            json_string = '''
            {
                "method": "read", 
                "params": {
                    "options": {
                        "channel": {
                            "name": "%s",
                            "members_type": "team",
                            "topic_name": "%s"
                        }, 
                        "pagination": {
                            "num": %d,
                            "next": "%s"
                        }
                    }
                }
            }
            '''  % (team_name, topic_name, 1000, next_page_arg)
            response = subprocess.check_output(["keybase", "chat", "api", "-m", json_string])
            response_json =  json.loads(response.decode('utf-8'))
            messages += response_json["result"]["messages"]
            another_page_exists = "last" not in response_json["result"]["pagination"].keys()
            if len(messages) >= 10000:
                json.dump(messages, open(f"{dm_save_dir}/{str(page_count).zfill(3)}.json", 'w'))
                sql_insert_list = []
                for message in messages:
                    sql_insert_list.append((team_name, topic_name, json.dumps(message),))
                self.cur.executemany("INSERT INTO team_messages_t(team_name, topic_name, message_json) VALUES(?, ?, json(?))", sql_insert_list)
                self.con.commit()
                messages = []
                page_count += 1
        json.dump(messages, open(f"{dm_save_dir}/{str(page_count).zfill(3)}.json", 'w'))
        sql_insert_list = []
        for message in messages:
               sql_insert_list.append((team_name, topic_name, json.dumps(message),))
        self.cur.executemany("INSERT INTO team_messages_t(team_name, topic_name, message_json) VALUES(?, ?, json(?))", sql_insert_list)
        self.con.commit()
        return True


    def save_all_group_chat_channels(self):
        if self.group_chats == None:
            self.save_list_group_chats()
        channel_list = []
        for channel in self.group_chats:
            if channel["channel"]["members_type"] == "impteamnative":
                channel_list.append(channel["channel"]["name"])
        for channel in channel_list:
            print(f"Getting messages for channel {channel}")
            self.save_all_messages_from_channel(channel, f"DMs/{channel}")
        return True


    def save_specific_team_chat(self, keybase_team):
        if self.teams == None:
            self.get_teams()
        if keybase_team not in self.teams:
            return "Error not a valid team"
        channels = self.get_team_channels(keybase_team)
        for channel in channels:
            self.save_all_messages_from_team_channel(keybase_team, channel, f"SpecificTeamOut/{keybase_team}/{channel}")
        return True

    def save_all_team_channel_messages(self):
        self.get_all_team_channels()
        channel_list = []
        for team_name in self.team_channels:
            for topic_name in self.team_channels[team_name]:
                channel_list.append({
                    "team_name" : team_name,
                    "topic_name" : topic_name
                })
        for channel in channel_list:
            tmp_team_name  = channel["team_name"]
            tmp_topic_name = channel["topic_name"]
            print(f"Getting messages from team {tmp_team_name} channel {tmp_topic_name}")
            try:
                fetch_files = self.save_all_messages_from_team_channel(tmp_team_name, tmp_topic_name , f"Teams/{tmp_team_name}/{tmp_topic_name}")
            except Exception as e:
                print(f"ERROR etting messages from team {tmp_team_name} channel {tmp_topic_name} \n{e}")
            print(fetch_files)
        return True

    def get_attachments_from_all_group_chats(self):
        sql_query = '''
            select DISTINCT(json_extract(message_json, '$.msg.channel.name')) from group_messages_t
        '''
        res = self.cur.execute(sql_query).fetchmany(size=10000)
        formatted_result = []
        for chat in res:
            formatted_result.append(chat[0])
        return formatted_result
        for chat in formatted_result:
            self.get_attachments_from_specific_group_chat(chat)
        return True

    def get_attachments_from_specific_group_chat(self, group_chat_name):
        sql_query = f'''
            SELECT  
                json_extract(message_json, '$.msg.id'), 
                json_extract(message_json, '$.msg.sender.uid'), 
                json_extract(message_json, '$.msg.sent_at_ms'), 
                json_extract(message_json, '$.msg.content.attachment.object.filename') 
            FROM group_messages_t
            WHERE 
                json_extract(message_json, '$.msg.content.type') IN ("attachment", "attachmentuploaded")
                AND json_extract(message_json, '$.msg.channel.name') = "{group_chat_name}";
        '''
        res = self.cur.execute(sql_query).fetchmany(size=10000)
        # keybase chat download [command options] <conversation> <attachment id> [-o filename]
        # Attachment_id is the nonce
        Path(f"{self.save_dir}Attachments").mkdir(parents=True, exist_ok=True)
        for file_row in res:
            list_me = [
                "keybase", 
                "chat", 
                "download", 
                f"'{group_chat_name}'", 
                f"{file_row[0]}", 
                "-o",
                f"{self.save_dir}Attachments/{file_row[1]}-{file_row[2]}-{file_row[3].replace(' ', '_')}"
                ]
            print(" ".join(list_me))
            response = subprocess.run(" ".join(list_me), shell=True)
        return True

    def get_attachments_from_all_group_chats(self):
        sql_query = f'''
            SELECT  
                DISTINCT
                    group_name
            FROM group_messages_t
        '''
        tmp_groups = self.cur.execute(sql_query).fetchmany(size=10000)
        for tmp_group in tmp_groups:
            self.get_attachments_from_specific_group_chat(tmp_group[0])
        return True


    def forward_all_attachments_from_index_to_group_chat(self, group_chat):
        sql_query = f'''
            SELECT  
                json_extract(message_json, '$.msg.id')
            FROM team_messages_t
            WHERE json_extract(message_json, '$.msg.content.type') = "attachment"
        '''
        tmp_groups = self.cur.execute(sql_query).fetchmany(size=10000)


    def list_all_git_repos(self):
        res = self.cur.execute("SELECT COUNT(*) FROM git_repos_t").fetchone()[0]
        if res != 0:
            return True
        response = subprocess.check_output(["keybase", "git", "list"],stderr=subprocess.STDOUT, encoding="utf-8")
        all_repos = response.split("team repos:")[0].split("\n")[1:] + response.split("team repos:")[1].split("\n")
        for repo_index in range(len(all_repos)):
            all_repos[repo_index] = list(filter(("").__ne__, all_repos[repo_index].split(" ")))
        all_repos = list(filter(([]).__ne__, all_repos))
        self.cur.executemany("INSERT INTO git_repos_t(git_repo_name, git_repo_path) VALUES(?, ?)", all_repos)
        self.con.commit()
        return True

    def clone_all_git_repos(self):
        sql_query = f'''
            SELECT  
                git_repo_name, git_repo_path
            FROM git_repos_t
        '''
        git_repos = self.cur.execute(sql_query).fetchmany(size=10000)
        for repo in git_repos:
            save_path = f"{self.save_dir}GitRepos/{repo[0].replace('/', '-')}"
            Path(save_path).mkdir(parents=True, exist_ok=True)
            subprocess.run(f"git clone {repo[1]} {save_path}", shell=True)
        return True

    def get_keybase_followers(self):
        res = self.cur.execute("SELECT COUNT(*) FROM followers_t").fetchone()[0]
        if res != 0:
            return True
        response = subprocess.check_output(["keybase", "list-followers"])
        formatted_team_member_list = []
        for item in str(response)[2:-5].split("\\n"):
            formatted_team_member_list.append((item,))
        self.cur.executemany("INSERT INTO followers_t(keybase_username) VALUES(?)", formatted_team_member_list)
        self.con.commit()
        return True

    def get_keybase_following(self):
        res = self.cur.execute("SELECT COUNT(*) FROM following_t").fetchone()[0]
        if res != 0:
            return True
        response = subprocess.check_output(["keybase", "list-following"])
        formatted_team_member_list = []
        for item in str(response)[2:-5].split("\\n"):
            formatted_team_member_list.append((item,))
        self.cur.executemany("INSERT INTO following_t(keybase_username) VALUES(?)", formatted_team_member_list)
        self.con.commit()
        return True

    def get_list_all_users(self):
        # followers
        list_all_users = []
        sql_query = f'''
            SELECT  
                DISTINCT keybase_username
            FROM followers_t
        '''
        users = self.cur.execute(sql_query).fetchmany(size=10000)
        for item in users:
            if item not in list_all_users:
                list_all_users.append(item[0])
        # following
        sql_query = f'''
            SELECT  
                DISTINCT keybase_username
            FROM following_t
        '''
        users = self.cur.execute(sql_query).fetchmany(size=10000)
        for item in users:
            if item not in list_all_users:
                list_all_users.append(item[0])
        # Individual Messages
        sql_query = f'''
            SELECT  
                DISTINCT json_extract(message_json, '$.msg.sender.username')
            FROM group_messages_t
        '''
        users = self.cur.execute(sql_query).fetchmany(size=10000)
        for item in users:
            if item not in list_all_users:
                list_all_users.append(item[0])
        # Team Messages
        sql_query = f'''
            SELECT  
                DISTINCT json_extract(message_json, '$.msg.sender.username')
            FROM team_messages_t
        '''
        users = self.cur.execute(sql_query).fetchmany(size=10000)
        for item in users:
            if item not in list_all_users:
                list_all_users.append(item[0])
        return list_all_users

    def get_all_user_metadata(self):
        list_all_users = self.get_list_all_users()
        for keybase_username in list_all_users:
            res = self.cur.execute(f"SELECT COUNT(*) FROM user_metadata_t WHERE keybase_username='{keybase_username}'").fetchone()[0]
            if res != 0:
                print(f"{keybase_username} metadata already archived")
            else:
                print(f"Fetching metadata for {keybase_username}")
                try:
                    result = str(subprocess.check_output(["keybase", "id", keybase_username], shell=True))
                    result = result.replace("'", '"')
                    res = self.cur.execute(f"INSERT INTO user_metadata_t(keybase_username,metadata) VALUES('{keybase_username}', '{result}')" )
                    self.con.commit()
                except Exception as e:
                    print(f"Error with get_all_user_metadata {keybase_username}")

        return True
