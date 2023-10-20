from typing import Literal, TypedDict, Union


class FromLangServer(TypedDict):
    cmd: Literal['run_sql', 'db_data_update', 'check_for_update','connection_id']
    data: Union[str,dict,int]
    

class ToLangServer(TypedDict):
    cmd: Literal['db_data','ip','no_update','connection_id','ip_python_started']
    data: Union[str,dict,int]