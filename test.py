from modules.ExportKeybase import ExportKeybase
from pprint import pprint

ex_key = ExportKeybase()

# print("get_attachments_from_all_group_chats")
# print(f"{ex_key.get_attachments_from_all_group_chats()}")


group_chat_test = 'dentropy,eatergiant'
result = ex_key.get_attachments_from_specific_group_chat(group_chat_test)
pprint(result)
# ex_key.get_attachments_from_group_chats()
