from modules.ExportKeybase import ExportKeybase

ex_key = ExportKeybase()

print(f"save_keybase_username:         {ex_key.save_keybase_username()}")
print(f"get_keybase_followers:         {ex_key.get_keybase_followers()}")
print(f"get_keybase_following:         {ex_key.get_keybase_following()}")
print(f"save_teams:                    {ex_key.save_teams()}")
print(f"save_members_of_all_teams:     {ex_key.save_members_of_all_teams()}")
print(f"save_all_team_channels:        {ex_key.save_all_team_channels()}")
print(f"save_list_group_chats:         {ex_key.save_list_group_chats()}")
print(f"save_all_group_chat_channels:  {ex_key.save_all_group_chat_channels()}")
print(f"save_all_team_channel_messages:{ex_key.save_all_team_channel_messages()}")

# ex_key.save_specific_team_chat()
# ex_key.save_specific_team_chat("dentropydaemon")
