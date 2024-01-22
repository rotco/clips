from typing import List, Optional, Union


def int_check(num: int, var_name: str) -> None:
    if not isinstance(num, int):
        raise ValueError(f"'{var_name}' value must be an integer")
    return True


def render_item_list_to_string(items: List[str]) -> str:
    if not isinstance(items, list):
        raise ValueError(f"expecting a list object, got: {list}")
    text = ''
    for item in items:
        if not isinstance(item, str):
            raise ValueError(f"expecting a string object, got: {item}")
        text += f"'{item.lower()}', "
    return text.strip(', ')