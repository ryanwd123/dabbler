#%%
import regex as re

re_load_install = re.compile(r"(^| )(load |install )$", flags=re.IGNORECASE)
re_prgama =  re.compile(r"(^| )(pragma )$", flags=re.IGNORECASE)
re_set_reset = re.compile(r"(^| )(set |reset )$", flags=re.IGNORECASE)
re_set_reset2 = re.compile(r"(^| )(set |reset )(local | session | global )$", flags=re.IGNORECASE,)
# re_table_comp_dot = re.compile(r".*(^| )(?:\L<table_operations>)(\w+( \w+)?, )*(?P<dotitems>(\w+\.)+)$", flags=re.IGNORECASE, table_operations=table_operations)
# re_col_ref_dot = re.compile(r".*(^| |\()(?P<dotitems>(\w+\.)+)$", flags=re.IGNORECASE | re.VERBOSE)
re_ending_characters = re.compile(r".*(^| |\()(?P<char>(\w+))$", flags=re.IGNORECASE)


table_operations = ['join', 'from', 'pivot', 'unpivot', 'alter table', 'insert into']

re_table_comp = re.compile(r""".*(^| )(?:\L<table_operations>)(\s+)(("[^"]+"|\w+)(\s+\w+)?, )*(?P<ident>\w*)$""", flags=re.IGNORECASE, table_operations=table_operations)

re_table_comp_dot = re.compile(
r"""
    (?(DEFINE)
        (?P<item> (
                " (?P<ident>[^"]+) ") 
                | (?P<ident>\w+)               
                )
        
        (?P<table_ident>("[^"]+"|\w+))
        (?P<table_ref> (?&table_ident)(\.(?&table_ident))*(\(\))? (?:\s+\w+)?)

    )                            
    (?:^|\s)              # Match the start of the string or whitespace
    (?:\L<table_operations>)     # Match any of the table-related keywords
    (?:\s+)
    (?:((?&table_ref)\s*,\s*)*)                 # Optional whitespace after the keyword
    (?:
        (
        (?&item)   
        \.)+        # Ends with a dot
    "?        #optional '"'
    )
    $                   # Match the end of the string
""", flags=re.IGNORECASE | re.VERBOSE, table_operations=table_operations)

re_col_ref_dot = re.compile(
r"""
    (?(DEFINE)
        (?P<item> (
                " (?P<ident>[^"]+) ") 
                | (?P<ident>\w+)               
                )
    )                            
    .*(^|\s|\()              # Match the start of the string or whitespace
    (?:
        (
        (?&item)   
        \.)+        # Ends with a dot
    "?        #optional '"'
    )
    $                   # Match the end of the string
""", flags=re.IGNORECASE | re.VERBOSE)



