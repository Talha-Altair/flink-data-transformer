"""
	10 million employee records:
		id, name, email, city, state, country.

	10K users:
		change email to different domain
		change city to different city
"""

from math import radians
import faker
import pandas as pd
import random

fake = faker.Faker()

NUM_ROWS = 10000


def generate_data(num_rows):

    name_list = [fake.name() for _ in range(num_rows)]
    email_list = [fake.email() for _ in range(num_rows)]
    city_list = [fake.city() for _ in range(num_rows)]
    state_list = [fake.state() for _ in range(num_rows)]
    country_list = [fake.country() for _ in range(num_rows)]
    id_list = [ i for i in range(num_rows)]

    df = pd.DataFrame({
        'id': id_list,
        'name': name_list,
        'email': email_list,
        'city': city_list,
        'state': state_list,
        'country': country_list
    })

    return df


def generate_migration_date(num_rows):

    employee_id = [ random.randint(1, num_rows) for _ in range(num_rows)]
    new_city_list = [fake.city() for _ in range(num_rows)]

    df = pd.DataFrame({
        'employee_id': employee_id,
        'new_city': new_city_list
    })

    return df


if __name__ == '__main__':

    # df = generate_data(NUM_ROWS)

    # df.to_csv('data/employee_data.csv', index=False)

    # df = generate_migration_date(NUM_ROWS)

    # df.to_csv('data/migration_data.csv', index=False)

    print("Done")
