from typing import List


def int_check(num: int, var_name: str) -> None:
    if not isinstance(num, int):
        raise ValueError(f"'{var_name}' value must be an integer")
    return True


def validation_check(validated: bool, name: str) -> None:
    if not validated:
        raise ValueError(f"Failed to validate: {name}")


def render_item_list_to_string(items: List[str]) -> str:
    if not isinstance(items, list):
        raise ValueError(f"expecting a list object, got: {list}")
    text = ""
    for item in items:
        if not isinstance(item, str):
            raise ValueError(f"expecting a string object, got: {item}")
        text += f"'{item.lower()}', "
    return text.strip(", ")


def check_execution_state(state: str) -> bool:
    if state != "SUCCEEDED":
        raise ValueError(f"Could not complete the Query Execution, state: {state}")
