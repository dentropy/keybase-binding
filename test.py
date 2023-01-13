from modules.ExportKeybase import ExportKeybase

ex_key = ExportKeybase()

# print(ex_key.save_members_of_all_teams())
# teams = ex_key.get_teams()
# print(teams)
# # ex_key.save_teams("./out")

# team_memberships = ex_key.get_members_of_team(teams[0])
# print(team_memberships)

print(ex_key.save_list_group_chats())
messages = ex_key.save_all_group_chat_channels()

print(messages)