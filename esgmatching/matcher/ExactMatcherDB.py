""" This module defines the class MatcherDbByExactId """

# Import python libraries


# Import internal libraries
from esgmatching.matcher import Matcher
from esgmatching.dbmanager import SqlUtility
from esgmatching.dbmanager.SqlEngine import SqlEngine
from esgmatching.entity.EntityDB import EntityDB
from esgmatching.report.ReportOnMatching import ReportOnMatching

from esgmatching.exceptions import NotConnectedToDatabase, ValueErrorDatabaseEngine, \
    ErrorDB, MatchingTableAlreadyExists, IndirectMatchingMustBeExecutedAfterExactMatching


class ExactMatcherDB(Matcher.Matcher):
    """
        Class that allows performing an exact matching by ID by using data stored in a database.

    """

    def __init__(self, matching_name, ref_data_source):
        """
            Constructor method.

            Parameters:
                ref_data_source (EntityDataSource): data source object used as reference
                matching_name (string): a descriptive name for the matching to be performed

            Returns:
                MatcherDbByExactId (object)
            Raises:
                No exception is raised.
        """
        super().__init__(matching_name, ref_data_source)
        self._db_engine = None
        self._matching_entity = None
        self._no_matching_entity = None
        self._report_on_matching = None

    @property
    def matching_entity(self):
        """
            Class property that defines the metadata used as keys to query the data source

            Parameters:

            Returns:
                matching_entity (EntityDB): a list with the name of key attributes.
            Raises:
                No exception is raised.
        """
        return self._matching_entity

    @property
    def no_matching_entity(self):
        """
            Class property that defines the metadata used as keys to query the data source

            Parameters:

            Returns:
                no_matching_entity (EntityDB): a list with the name of key attributes.
            Raises:
                No exception is raised.
        """
        return self._no_matching_entity

    @property
    def report_on_matching(self):
        """
            Class property that defines the metadata used as keys to query the data source

            Parameters:

            Returns:
                report_on_matching (ReportOnMatching): a list with the name of key attributes.
            Raises:
                No exception is raised.
        """
        return self._report_on_matching

    def set_database_engine(self, sql_engine_obj):
        """
            Class method that allows the injection of an object engine, as to perform operations in a database.

            Parameters:
                sql_engine_obj (SqlEngine): the object engine connected to a database.

            Returns:

            Raises:
                AttributeError: if the object engine is not connected to the database or is of a wrong type
        """
        if type(sql_engine_obj) is SqlEngine:
            if sql_engine_obj.is_connected():
                self._db_engine = sql_engine_obj
            else:
                raise NotConnectedToDatabase
        else:
            raise ValueErrorDatabaseEngine

    def __get_target_attributes_and_types(self):
        # Column names and types in the target data sources
        target_attributes = {}
        type_target_attributes = []
        for target_ds in self._target_data_sources:
            for original_attribute, renamed_attribute in target_ds.dict_attributes.items():
                if renamed_attribute not in list(target_attributes.values()):
                    target_attributes[original_attribute] = renamed_attribute
                    type_target_attributes.append(SqlUtility.get_db_type('str'))
        return target_attributes, type_target_attributes

    def __get_ref_attributes_and_types(self):
        # Column names and types in the reference data source
        # Add only the attributes in common with the target data sources as to facilitate comparisons
        ref_attributes = {}
        type_ref_attributes = []
        for original_attribute, renamed_attribute in self._ref_data_source.dict_attributes.items():
            ref_attributes[original_attribute] = 'ref_' + renamed_attribute
            type_ref_attributes.append(SqlUtility.get_db_type('str'))

        return ref_attributes, type_ref_attributes

    def __create_matching_entity(self, table_name):
        """
            Private class method that creates the matching table in the database.

            Parameters:
                table_name (string): the name of the matching table

            Returns:
                new_table (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Raises:
                No exception is raised.
        """
        try:
            # Some standard column names and types
            db_str_type = SqlUtility.get_db_type('str')
            db_int_type = SqlUtility.get_db_type('int')
            table_columns = ['match', 'target_name', 'ref_name']
            types_columns = [db_int_type, db_str_type, db_str_type]

            # Column names and types in the target data sources
            [target_attributes, type_target_attributes] = self.__get_target_attributes_and_types()

            # Column names and types in the reference data source
            # Add only the attributes in common with the target data sources as to facilitate comparisons
            [ref_attributes, type_ref_attributes] = self.__get_ref_attributes_and_types()

            # Add all attributes from reference and target data sources together
            table_columns = table_columns + list(target_attributes.values()) + list(ref_attributes.values())
            types_columns = types_columns + type_target_attributes + type_ref_attributes

            # Finally, create the matching table
            # Set the add_timestamp = True as to create an automatic datetime attribute
            new_table = self._db_engine.create_table(table_name, table_columns, types_columns, True, True)

            # Create the matching entity
            matching_entity = EntityDB(new_table)
            matching_entity.set_database_engine(self._db_engine)

            # Add the attributes to the matching entity
            matching_attributes = target_attributes.update(ref_attributes)
            matching_entity.dict_attributes = matching_attributes

            return matching_entity

        except Exception as e:
            original_msg = getattr(e, 'message', repr(e))
            exception_msg = original_msg + ' - ' + 'Error creating table [{}].'
            raise ErrorDB(exception_msg)

    def __create_no_matching_entity(self, table_name):
        """
            Private class method that creates the non matching table in the database.

            Parameters:
                table_name (string): the name of the non matching table

            Returns:
                new_table (sqlalchemy.sql.schema.Table): metadata object that represents the table in the database

            Raises:
                No exception is raised.
        """
        try:
            # Some standard column names and types
            table_columns = ['target_name']
            types_columns = [SqlUtility.get_db_type('str')]

            # Column names and types in the target data sources
            [target_attributes, type_target_attributes] = self.__get_target_attributes_and_types()

            # Add all attributes from reference and target data sources together
            table_columns = table_columns + list(target_attributes.values())
            types_columns = types_columns + type_target_attributes

            # Finally, create the matching table
            # Set the add_timestamp = True as to create an automatic datetime attribute
            new_table = self._db_engine.create_table(table_name, table_columns, types_columns, True, True)

            # Create the no matching entity
            no_matching_entity = EntityDB(new_table)
            no_matching_entity.set_database_engine(self._db_engine)

            # Add the attributes to the no matching entity
            no_matching_entity.dict_attributes = target_attributes

            return no_matching_entity

        except Exception as e:
            original_msg = getattr(e, 'message', repr(e))
            exception_msg = original_msg + ' - ' + 'Error creating table [{}].'
            raise ErrorDB(exception_msg)

    def execute_matching(self, matching_table_name='', matching_desc='', drop_table=True):
        """
            Class method that performs the matching between the target data sources and a referential source

            Parameters:
                matching_table_name (string): the name of the matching table
                matching_desc (string): description of the matching
                drop_table (bool): flag that indicates to drop the table if it already exists.

            Returns:

            Raises:
                No exception is raised.
        """
        try:
            # Check if the matching table exists, and drop it as to create a new one
            if self._db_engine.table_exists(matching_table_name):
                if drop_table:
                    table_obj = self._db_engine.get_table_from_metadata(matching_table_name)
                    self._db_engine.drop_table(table_obj)
                    table_obj = self._db_engine.get_table_from_metadata('No_' + matching_table_name)
                    self._db_engine.drop_table(table_obj)
                else:
                    raise MatchingTableAlreadyExists

            # Create the matching entity
            matching_entity = self.__create_matching_entity(matching_table_name)
            matching_table_obj = matching_entity.table_obj

            # Create the no-matching entity
            no_matching_entity = self.__create_no_matching_entity('No_' + matching_table_name)
            no_matching_table_obj = no_matching_entity.table_obj

            # Get the referential table object and its column objects
            table_ref_obj = self._ref_data_source.table_obj
            ref_columns = self._ref_data_source.get_table_columns_from_renamed_attributes('ref_')
            ref_columns['ref_name'] = self._ref_data_source.get_table_column('data_source')

            # Convert the referential columns to database columns with labels or alias
            # The SqlUtility will convert a dictionary in a list of database column objects with alias, such as:
            # data_source_ref.company_name AS ref_company_name
            ref_columns_db = SqlUtility.get_db_columns_with_labels(ref_columns)

            # Create the reporting object
            result_report = ReportOnMatching(self._name, matching_desc)

            # Process the matching for each target data source
            for ds_target in self._target_data_sources:
                # Get the target object and its columns object
                table_target_obj = ds_target.table_obj
                target_columns = ds_target.get_table_columns_from_renamed_attributes()
                target_columns['target_name'] = ds_target.get_table_column('data_source')
                target_columns_db = SqlUtility.get_db_columns_with_labels(target_columns)

                # Concatenate all columns from target and referential
                select_columns_db = target_columns_db + ref_columns_db
                matching_columns = list(target_columns.keys()) + list(ref_columns.keys())

                # Get the matching keys to be used in the join clause
                # The matching keys will be the attributes in common between the referencial and the target
                if len(self._matching_keys) == 0:
                    matching_keys = self._ref_data_source.get_matching_keys(ds_target.keys)
                else:
                    matching_keys = self._matching_keys
                print(matching_keys)
                matching_keys_obj = {}
                # Convert the matching keys into column objects
                for key in matching_keys:
                    key_ref = self._ref_data_source.get_table_column(key)
                    key_target = ds_target.get_table_column(key)
                    matching_keys_obj[key] = [key_target, key_ref]

                # Get the where clause. Each item in matching_keys_obj is a where clause.
                # Therefore, the get_multi_where_clause() method connects them together by adding
                # the sql operator AND
                where_clause_obj = SqlUtility.get_multi_where_clause(matching_keys_obj)

                # Get the select...join statement object, such as:
                # SELECT [select_columns_db] FROM [table_target_obj] JOIN [table_ref_obj] ON [where_clause_obj]
                join_stm = SqlUtility.get_query_join(table_target_obj, table_ref_obj,
                                                     select_columns_db, where_clause_obj)
                # Insert the values in the matching table
                # INSERT INTO [matching_table_obj] [matching_columns] [join_stm]
                self._db_engine.insert_into_from_select(matching_table_obj, matching_columns, join_stm)

                # Update the matching flag
                matching_flag_obj = matching_table_obj.columns['match']
                self._db_engine.update_where_is_null(matching_table_obj, matching_flag_obj, 1)

                # Compute matching coverage
                total_dstarget_entries = self._db_engine.get_count_by_column(ds_target.table_obj.columns['idx'])
                col = matching_table_obj.columns['target_name']
                filter_col = "'{}'".format(ds_target.table_name)
                total_matching_entries = self._db_engine.get_count_by_column_where_literal(col, filter_col)
                total_matching = (total_matching_entries / total_dstarget_entries) * 100

                # Add the data to the report matching
                result_report.add_matching_data(self._ref_data_source.table_name,
                                                ds_target.table_name, total_matching)

                # Compute the non-matching table which is the reverse of the matching
                # Get the where clause. Each item in matching_keys_obj is a where clause.
                # Therefore, the get_multi_where_clause() method connects them together by adding
                # the sql operator AND
                keys_is_null = []
                for key in matching_keys:
                    key_ref = self._ref_data_source.get_table_column(key)
                    keys_is_null.append(key_ref)
                where_clause_isnull = SqlUtility.get_multi_where_clause_isnull(keys_is_null)
                join_stm = SqlUtility.get_query_join_where_clause(table_target_obj, table_ref_obj,
                                                                  target_columns_db, where_clause_obj,
                                                                  where_clause_isnull)
                # Insert the values in the matching table
                # INSERT INTO [matching_table_obj] [matching_columns] [join_stm]
                self._db_engine.insert_into_from_select(no_matching_table_obj, list(target_columns.keys()), join_stm)

            # Update the class attributes with the entities resultant of the matching
            self._matching_entity = matching_entity
            self._no_matching_entity = no_matching_entity

            # Update the class attribute that stores the report on matching
            self._report_on_matching = result_report

            return self._report_on_matching

        except Exception as e:
            original_msg = getattr(e, 'message', repr(e))
            exception_msg = original_msg + ' - ' + 'Error in matching function.'
            raise ErrorDB(exception_msg)

    def execute_indirect_matching(self):
        if self._matching_entity is None or self._no_matching_entity is None:
            raise IndirectMatchingMustBeExecutedAfterExactMatching

        # The matching-table plays the role of the referential and the no-matching tables is the target
        table_target_obj = self._no_matching_entity.table_obj
        table_ref_obj = self._matching_entity.table_obj

        matching_columns = self._matching_entity.get_table_columns_from_db()
        matching_columns.pop('match')
        # matching_columns = list(matching_columns.keys())
        matching_columns = ['target_name', 'isin', 'lei', 'company_name', 'country', 'sedol', 'ref_isin',
                            'ref_company_name', 'ref_country', 'ref_name']
        print(matching_columns)

        target_columns = self._no_matching_entity.get_table_columns_from_db()
        print(target_columns)
        target_columns_db = SqlUtility.get_db_columns_with_labels(target_columns)

        ref_columns = self._matching_entity.get_table_columns_from_db(prefix_alias='ref_', not_with_prefix='ref_')
        ref_columns.pop('ref_match')
        ref_columns.pop('ref_target_name')
        ref_columns.pop('ref_lei')
        ref_columns.pop('ref_sedol')
        ref_columns['ref_name'] = self._matching_entity.get_table_column('target_name')
        print(ref_columns)
        ref_columns_db = SqlUtility.get_db_columns_with_labels(ref_columns)

        all_columns_db = target_columns_db + ref_columns_db

        for ds_target in self._target_data_sources:
            matching_columns = ['target_name', 'isin', 'lei', 'company_name', 'country', 'sedol', 'ref_isin',
                                'ref_company_name', 'ref_country', 'ref_name']
            # Get the indirect matching keys to be used in the join clause
            # The indirect matching keys is a list in the metadata file under the name called 'other_keys'
            matching_keys = ds_target.other_keys
            print(matching_keys)
            if matching_keys is None or len(matching_keys) == 0:
                continue
            matching_keys_obj = {}
            # Convert the indirect matching keys into column objects
            for key in matching_keys:
                # The idea is to do the indirect matching between the no-matching and matching tables
                # The matching-table plays the role of the referential and the no-matching tables is the target
                key_ref = self._matching_entity.get_table_column(key)
                key_target = self._no_matching_entity.get_table_column(key)
                matching_keys_obj[key] = [key_target, key_ref]

            # Get the join clause in which each item in matching_keys_obj is a condition.
            # Therefore, the get_multi_where_clause() method connects them together by adding
            # the sql operator AND, as demonstrated in the example below:
            # No_Matching_Table.isin = Matching_Table.isin AND No_Matching_Table.lei = Matching_Table.lei
            join_clause_obj = SqlUtility.get_multi_where_clause(matching_keys_obj)

            # Get the where clause, restricting the SELECT by the name of data_source and if there was
            # an exact matching (match=1):
            # WHERE Matching_Table.match=1 AND No_Matching_Table.target_name =<ds_target_name>
            literal_ds_target_name = "'{}'".format(ds_target.table_name)
            match_column = self._matching_entity.get_table_column('match')
            target_name_column = self._no_matching_entity.get_table_column('target_name')
            where_clause_dict = {'match': [match_column, '1'],
                                 'target_name': [target_name_column, literal_ds_target_name]}
            where_clause_obj = SqlUtility.get_multi_where_clause(where_clause_dict)

            # Get the complete select...join...where statement object, such as:
            # SELECT [select_columns_db] FROM [table_target_obj] JOIN [table_ref_obj] ON [join_clause_obj]
            # WHERE [where_clause_obj]
            join_stm = SqlUtility.get_query_join(table_target_obj, table_ref_obj,
                                                 all_columns_db, join_clause_obj, where_clause_obj)

            # Insert the values in the matching table
            # INSERT INTO [matching_table_obj] [matching_columns] [join_stm]
            # This first INSERT INTO adds the result of the join between two data sources: e.g. DS1 e DS2
            # DS1 is present in the matching_table and DS2 is from the no-matching-table
            self._db_engine.insert_into_from_select(table_ref_obj, matching_columns, join_stm)

            # This second INSERT INTO adds the result of the join between the data source in the no-matching-table
            # DS2 and the referential connected to DS1 in the matching table; closing the cycle DS2 -> DS1 -> REF
            # Therefore, DS2 will appear connected to REF in the final matching table.
            matching_columns = ['target_name', 'isin', 'lei', 'company_name', 'country', 'sedol', 'ref_name',
                                'ref_isin', 'ref_company_name', 'ref_country']
            ref_columns2 = self._matching_entity.get_table_columns_from_db(with_prefix='ref_')
            print(ref_columns2)
            ref_columns2_db = SqlUtility.get_db_columns_with_labels(ref_columns2)
            all_columns2_db = target_columns_db + ref_columns2_db
            join_stm = SqlUtility.get_query_join(table_target_obj, table_ref_obj,
                                                 all_columns2_db, join_clause_obj, where_clause_obj)
            self._db_engine.insert_into_from_select(table_ref_obj, matching_columns, join_stm)

            # Update the matching flag
            self._db_engine.update_where_is_null(table_ref_obj, match_column, 2)