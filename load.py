import csv
import psycopg2
from config import config


def insert_weo(country, subject_descriptor, units, scale, y2017, y2018, y2019, y2020, y2021, y2022):
    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO public.weo(
	country, subject_descriptor, units, scale, y2017, y2018, y2019, y2020, y2021, y2022)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING country;"""

    conn = None
    country_s = None
    try:
        # readdatabase configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (country, subject_descriptor, units, scale, y2017, y2018, y2019, y2020, y2021, y2022))
        # get the generated id back
        country_s = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return country_s
def insert_bli(country, dwellings_without_basic_facilities, housing_expenditure, rooms_per_person, household_net_income, household_net_wealth, labour_market_insecurity, employment_rate, longterm_unemployment_rate, personal_earnings, quality_support_network, educational_attainment, student_skills, years_in_education, air_pollution, water_quality, stakeholder_engagement_for_developing_regulations, voter_turnout, selfreported_health, life_satisfaction, feeling_safe_walking_alone_at_night, homicide_rate, employees_working_very_long_hours, time_devoted_to_leisur_and_personal_care):
    """ insert a new bli into the bli table """
    sql = """INSERT INTO public.bli(
	country, dwellings_without_basic_facilities, housing_expenditure, rooms_per_person, household_net_income, household_net_wealth, labour_market_insecurity, employment_rate, longterm_unemployment_rate, personal_earnings, quality_support_network, educational_attainment, student_skills, years_in_education, air_pollution, water_quality, stakeholder_engagement_for_developing_regulations, voter_turnout, selfreported_health, life_satisfaction, feeling_safe_walking_alone_at_night, homicide_rate, employees_working_very_long_hours, time_devoted_to_leisur_and_personal_care)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING country;"""

    conn = None
    country_s = None
    try:
        # readdatabase configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (country, dwellings_without_basic_facilities, housing_expenditure, rooms_per_person, household_net_income, household_net_wealth, labour_market_insecurity, employment_rate, longterm_unemployment_rate, personal_earnings, quality_support_network, educational_attainment, student_skills, years_in_education, air_pollution, water_quality, stakeholder_engagement_for_developing_regulations, voter_turnout, selfreported_health, life_satisfaction, feeling_safe_walking_alone_at_night, homicide_rate, employees_working_very_long_hours, time_devoted_to_leisur_and_personal_care))
        # get the generated id back
        country_s = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return country_s


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def readWEDCSV():
    with open('WEO_Data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                #print(f'Column names are {", ".join(row)}')

                line_count += 1
            else:
                #print(f'\t{row[0]} works in the {row[1]} department, and was born in {row[2]}.')
                #print(row[0])
                country = insert_weo(row[0],
                           row[1],
                           row[2],
                           row[3],
                           row[4],
                           row[5],
                           row[6],
                           row[7],
                           row[8],
                           row[9]
                           )

                print(country)

                line_count += 1
        print(f'Processed {line_count} lines.')
def readBLICSV():
    with open('BLI_Data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                #print(f'Column names are {", ".join(row)}')

                line_count += 1
            else:
                #print(f'\t{row[0]} works in the {row[1]} department, and was born in {row[2]}.')
                #print(row[0])
                country = insert_bli(row[0].replace('  ',''),
                           row[1],
                           row[2],
                           row[3],
                           row[4],
                           row[5],
                           row[6],
                           row[7],
                           row[8],
                           row[9],
                           row[10],
                           row[11],
                           row[12],
                           row[13],
                           row[14],
                           row[15],
                           row[16],
                           row[17],
                           row[18],
                           row[19],
                           row[20],
                           row[21],
                           row[22],
                           row[23]


                           )

                print(country)

                line_count += 1
        print(f'Processed {line_count} lines.')

if __name__ == '__main__':
    connect()
    #readWEDCSV()
    readBLICSV()

