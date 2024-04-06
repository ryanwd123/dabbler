#%%

def line_col(str:str, idx:int):
    return str.count('\n', 0, idx), idx - str.rfind('\n', 0, idx)-1

def get_idx(txt,cur_line,cur_col):
    x = sum(len(x)+1 for i,x in enumerate(txt.split('\n'),start=0) if i < cur_line)
    return x + cur_col

def move_line(txt:str, idx:int, anchor:int, direction:int):
    row, col = line_col(txt, idx)
    arow, acol = line_col(txt, anchor)
    start = min(row,arow)
    end = max(row,arow)
    lines = txt.split('\n')
    print(start,end,idx,anchor)
    if direction == -1:
        if min(row,arow) == 0:
            return None
        else:
            new_txt = '\n'.join(lines[0:start - 1] + lines[start:end+1] + [lines[start-1]] + lines[end+1:])
            new_idx = get_idx(new_txt, row-1, col)
            new_anchor = get_idx(new_txt, arow-1, acol)
            return new_idx, new_anchor, new_txt
    if direction == 1:
        if max(row,arow) + 1 == len(lines):
            return None
        else:
            new_txt = '\n'.join(lines[0:start] + [lines[end+1]] + lines[start:end+1]  + lines[end+2:])
            new_idx = get_idx(new_txt, row+1, col)
            new_anchor = get_idx(new_txt, arow+1, acol)
            return new_idx, new_anchor, new_txt
            

# %%
