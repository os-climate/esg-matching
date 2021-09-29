import sqlalchemy as db
import sqlalchemy.sql as sql


def get_value_db(value):
    """
        Class method that checks if a value is null and returns the equivalent database null object.
        Otherwise, returns the value without change.

        Parameters:

        Returns:
            database sql.null() object or the unchanged value.

        Raises:
            No exception is raised.
    """
    value = str(value).strip()
    if len(value) == 0:
        return sql.null()
    else:
        return value


def get_db_type(str_type):
    """
        Class method that returns the equivalent database type, given a string that defines its type.

        Parameters:
            str_type (string): a string that defines a datatype

        Returns:
            the database object type (db.String, db.Integer, db.Boolean, db.DateTime)

        Raises:
            No exception is raised.
    """
    if str_type == 'str':
        return db.String
    elif str_type == 'int':
        return db.Integer
    elif str_type == 'bol':
        return db.Boolean
    elif str_type == 'datetime':
        return db.DateTime
    else:
        return None


def get_function_where_clause(column_obj, type_function):
    where_clause_obj = None
    if type_function == 'null':
        where_clause_obj = sql.expression.Select.where(column_obj.is_(None))
    elif type_function == 'not_null':
        where_clause_obj = sql.expression.Select.where(column_obj.isnot(None))
    return where_clause_obj


def get_single_where_clause(value_a, value_b):
    where_clause_obj = sql.expression.ColumnOperators.__eq__(value_a, value_b)
    return where_clause_obj


def get_single_where_clause_notequal(value_a, value_b):
    where_clause_obj = sql.expression.ColumnOperators.__ne__(value_a, value_b)
    return where_clause_obj


def get_multi_where_clause(columns_dict):
    where_clause_obj = None
    for columns in columns_dict:
        columns_a = columns_dict[columns][0]
        columns_b = columns_dict[columns][1]
        if not isinstance(columns_b, db.Column):
            columns_b = db.text(columns_b)
        if where_clause_obj is None:
            where_clause_obj = get_single_where_clause(columns_a, columns_b)
        else:
            where_clause_obj = sql.and_(where_clause_obj,
                                        get_single_where_clause(columns_a, columns_b))
    return where_clause_obj


def get_multi_where_clause_notequal(columns_dict):
    where_clause_obj = None
    for columns in columns_dict:
        columns_a = columns_dict[columns][0]
        columns_b = columns_dict[columns][1]
        if where_clause_obj is None:
            where_clause_obj = get_single_where_clause_notequal(columns_a, columns_b)
            print(where_clause_obj)
        else:
            where_clause_obj = sql.and_(where_clause_obj,
                                        get_single_where_clause_notequal(columns_a, columns_b))
            print(where_clause_obj)
    return where_clause_obj


def get_db_columns_with_labels(ref_columns):
    columns_with_labels = []
    for column in ref_columns:
        columns_with_labels.append(ref_columns[column].label(column))
    return columns_with_labels


def get_query_join(main_table_obj, link_table_obj, select_columns, join_clause, where_clause=None):
    j = main_table_obj.join(link_table_obj, join_clause)
    if where_clause is None:
        join_stm = sql.select(select_columns).select_from(j)
    else:
        join_stm = sql.select(select_columns).select_from(j).where(where_clause)
    return join_stm


def get_multi_where_clause_isnull(columns_list):
    where_clause_obj = None
    for column in columns_list:
        if where_clause_obj is None:
            where_clause_obj = column.is_(None)
        else:
            where_clause_obj = sql.and_(where_clause_obj, column.is_(None))
    return where_clause_obj


def get_query_join_where_clause(main_table_obj, link_table_obj, select_columns, join_clause, where_clause):
    j = main_table_obj.join(link_table_obj, join_clause, isouter=True)
    join_stm = sql.select(select_columns).select_from(j).where(where_clause)
    return join_stm


def is_table_object(table_obj):
    if isinstance(table_obj, db.Table):
        return True
    else:
        return False
