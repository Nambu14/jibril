import getopt
import sys

import requests
import pandas


def get_zipcode_list(dataframe):
    zip_code_list = []
    for zip_code in dataframe['zipcode']:
        try:
            zip_code_list.index(zip_code)
        except ValueError:
            zip_code_list.append(zip_code)
            pass
    return zip_code_list


def get_dictionaries(zip_code_list):
    latitude_dict = {}
    longitude_dict = {}
    state_dict = {}
    city_dict = {}
    request = 'http://www.zipcodeapi.com/rest/{api_key}/info.{output_format}/{zip_code}/{units}'
    api_key = 'gKxhSFNLFqK1TBj1VpLmx7aeqdeEN1uj2kUgqZE199lcBql68RHjzpgnxJ8vqd3O'
    output_format = 'json'
    units = 'degrees'

    for zip_code in zip_code_list:
        resp = requests.get(
            request.format(api_key=api_key, output_format=output_format, zip_code=zip_code, units=units))
        if resp.status_code != 200:
            # This means something went wrong.
            latitude_dict[zip_code] = ''
            longitude_dict[zip_code] = ''
            state_dict[zip_code] = ''
            city_dict[zip_code] = ''
        else:
            data = resp.json()
            latitude_dict[zip_code] = data['lat']
            longitude_dict[zip_code] = data['lng']
            state_dict[zip_code] = data['city']
            city_dict[zip_code] = data['state']

    return latitude_dict, longitude_dict, state_dict, city_dict


def write_output(dataframe):
    dataframe.to_excel('orders_enriched.xlsx', 'sh_out', index=False)


def jibril(input_file):
    orders = pandas.read_excel(io=input_file)
    orders_df = pandas.DataFrame(orders)
    zip_list = get_zipcode_list(orders_df)
    latitude_dict, longitude_dict, state_dict, city_dict = get_dictionaries(zip_list)

    orders_df['state'] = ''
    orders_df['city'] = ''
    orders_df['latitude'] = ''
    orders_df['longitude'] = ''

    for index, row in orders_df.iterrows():
        orders_df.at[index, 'latitude'] = latitude_dict[row['zipcode']]
        orders_df.at[index, 'longitude'] = longitude_dict[row['zipcode']]
        orders_df.at[index, 'state'] = state_dict[row['zipcode']]
        orders_df.at[index, 'city'] = city_dict[row['zipcode']]

    write_output(orders_df)


def usage():
    print('Usage: jibril.py')
    print('       Loads data from bson files to target db\n')
    print('Arguments: --input_file        - The to be processed')

    sys.exit()


def main(argv):
    supported_args = """input_file="""
    optlist = []

    # Extract parameters
    try:
        optlist, remaining = getopt.getopt(argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        print(str(err))
        usage()

    # Parse parameters
    kwargs = {}
    optlist_flag = 0

    for arg, value in optlist:
        optlist_flag = 1
        if arg == "--input_file":
            if value == '':
                usage()
            else:
                kwargs['input_file'] = value
        else:
            usage()
        jibril(kwargs['input_file'])

    if optlist_flag == 0:
        usage()


if __name__ == '__main__':
    main(sys.argv)
