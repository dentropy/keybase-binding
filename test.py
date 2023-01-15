from modules.ExportKeybase import ExportKeybase
from pprint import pprint

ex_key = ExportKeybase()

# print("get_attachments_from_all_group_chats")
# print(f"{ex_key.get_attachments_from_all_group_chats()}")


# group_chat_test = 'dentropy,eatergiant'
# result = ex_key.get_attachments_from_specific_group_chat(group_chat_test)
# pprint(result)


# result = ex_key.get_attachments_from_all_group_chats()
# pprint(result)

# ex_key.get_list_all_users()
ex_key.get_all_user_metadata()
