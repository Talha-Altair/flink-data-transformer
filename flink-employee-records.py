"""
    Author: Altair

"""
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

PATH_TO_EMPLOYEE_DATA = './data/employee_data.csv'
PATH_TO_MIGRATION_DATA = './data/migration_data.csv'
NEW_DOMAIN_NAME = '@altair.com'

pandas_df = pd.read_csv(PATH_TO_EMPLOYEE_DATA)
migration_df = pd.read_csv(PATH_TO_MIGRATION_DATA)

class change_email(object):

    def __init__(self):

        self.domain = NEW_DOMAIN_NAME

    def __call__(self, email):

        apex = email.split('@')[0]

        email = apex + self.domain

        return email

class change_city(object):

    def __init__(self):

        self.name = 'Altair'

    def __call__(self, employee_id, city):

        required_df = migration_df[str(migration_df['employee_id']) == employee_id]

        if required_df.empty:

            return city

        return required_df['new_city'].values[0]


def main():

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    table = t_env.from_pandas(pandas_df)
    # table_migration = t_env.from_pandas(migration_df)

    change_email_udf = udf(change_email(), result_type=DataTypes.STRING())
    change_city_udf = udf(change_city(), result_type=DataTypes.STRING())

    t_env.create_temporary_function("change_email", change_email_udf)
    t_env.create_temporary_function("change_city", change_city_udf)

    t_env.create_temporary_view('employees', table)
    # t_env.create_temporary_view('migrations', table_migration)

    emaiL_updated_table = t_env.sql_query("SELECT id, name, change_email(email) as email , change_city(id, city), state, country \
                             FROM employees \
                            ")

    # t_env.create_temporary_view('emaiL_updated_employees', emaiL_updated_table)

    # if_statement = " IF (migrations.id = emaiL_updated_employees.id) "

    # final_table = t_env.sql_query("SELECT id, name, email, IF(), state, country \
    #                             FROM emaiL_updated_employees \
    #                             ")

    result_df = emaiL_updated_table.to_pandas()

    print(result_df)

    return 0


if __name__ == "__main__":

    main()
