def operators(record, _operator, value):
    if str(_operator) == "=":
        return str(record) == str(value)
    if str(_operator) == "<":
        return str(record) < str(value)
    if str(_operator) == ">":
        return str(record) > str(value)