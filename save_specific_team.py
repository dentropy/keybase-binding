from modules.ExportKeybase import ExportKeybase

ex_key = ExportKeybase()

team_name = "dentropydaemon"

ex_key.save_team_members(team_name)
ex_key.save_team_channels(team_name)
ex_key.save_specific_team_chat(team_name)
