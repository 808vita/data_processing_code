import os
import pandas as pd
import re
import copy
import json

bundled_data_path = "./bundled_data/"
print(os.listdir(bundled_data_path))

processed_data_path = "./processed_data/"

overview_bundled_file = "overview.csv"
reviews_bundled_file = "detailed_reviews.csv"

file_name_dict = {"overview": overview_bundled_file, "reviews": reviews_bundled_file}


def generate_csv_file_path(file_name):
    """geneate csv file path

    Args:
        file_name (string): csv file name

    Returns:
        string: current csv file path
    """

    current_csv_file_path = bundled_data_path + file_name

    return current_csv_file_path


def read_csv(file_name):
    """generate full file path & returns pandas dataframe

    Args:
        file_name (string): csv file names

    Returns:
        pandas_dataframe: reads current csv file & returns pandas dataframe
    """
    current_file_full_path = bundled_data_path + file_name

    current_csv = pd.read_csv(current_file_full_path)

    print(current_csv.columns)
    print(current_csv.shape)

    return current_csv


def example_regex_lat_lng_operation():
    """example regex operation
    | commented out part works only partially
    """
    txt = "https://www.google.com/maps/place/Viveks/data=!4m7!3m6!1s0x3a5266581aaa967d:0xd97b39f78e122893!8m2!3d13.0472964!4d80.2327796!16s%2Fg%2F11b66gnzrj!19sChIJfZaqGlhmUjoRkygSjvc5e9k?authuser=0&hl=en&rclk=1"

    # x = re.search(r"!3d(-?\d+(?:\.\d+)?)!4d(-?\d+(?:\.\d+))", txt)
    # print(x)

    # lat, lng = x.string.split("!3d")[1].split("!4d")

    # print("lat , lng ", lat, " ", lng)

    x = re.split(r"!3d(-?\d+(?:\.\d+)?)!4d(-?\d+(?:\.\d+))", txt)
    print(x)

    lat, lng = x[1], x[2]

    print("lat , lng ", lat, " ", lng)


example_regex_lat_lng_operation()


def regex_extract_lat_lng(map_url):

    map_url_regex_split = re.split(r"!3d(-?\d+(?:\.\d+)?)!4d(-?\d+(?:\.\d+))", map_url)

    lat, lng = map_url_regex_split[1], map_url_regex_split[2]

    return {"lat": lat, "lng": lng}


def overview_drop_columns(current_csv):
    """drops multiple columns in overview file

    Args:
        current_csv (padas_dataframe): overview file - pandas dataframe

    Returns:
        pandas dataframe: overview pandas dataframe after removing unrequired columns
    """

    overview_drop_columns_list = [
        "main_category",
        "categories",
        "query",
        "description",
        "is_spending_on_ads",
        "competitors",
        "website",
        "can_claim",
        "owner_name",
        "owner_profile_link",
        "featured_image",
        "workday_timing",
        "is_temporarily_closed",
        "closed_on",
        "phone",
        "review_keywords",
    ]

    return current_csv.drop(overview_drop_columns_list, axis=1)


def overview_remove_by_store_names(current_csv):
    """remove stores from list by using store names. Uses query operation to achieve instead of using 'isin' & inverting.
    much clearer

    Args:
        current_csv (pandas dataframe): overview csv file

    Returns:
        pandas dataframe: overview pandas dataframe after removal of specific stores from the datatframe.
    """
    # this list used in the query operation
    remove_store_name_list = [
        "Vivek Private Limited - Corporate Office",
        "Reliance Jio Digital Xpress Mini",
        "Reliance Digital Express Mini",
        "Reliance Digital Xpress Mini",
        "Vivek service centre",
        "Vivek Electricals",
        "My Jio Store",
        "Jio Digital Life",
        "Reliance Digital Xpress Mini And Jio Store",
    ]

    return current_csv.query("name not in @remove_store_name_list")


def convert_link_to_lat_lng_lists(current_csv):
    """convert links to lat lng lists

    Args:
        current_csv (pandas dataframe): overview pandas dataframe

    Returns:
        dict: a dictionary of 'lat_list' & 'lng_list'
    """
    links_list = current_csv["link"].to_list()

    lat_list = []
    lng_list = []

    for map_url in links_list:
        lat_lng_dict = regex_extract_lat_lng(map_url)
        lat_list.append(lat_lng_dict["lat"])
        lng_list.append(lat_lng_dict["lng"])

    return {"lat_list": lat_list, "lng_list": lng_list}


def add_lat_lng_columns_after_extracting_from_links(current_csv):
    """extracs lat lng lists & adds to the pandas dataframe - creates two columns lat & lng

    Args:
        current_csv (pandas dataframe): overview pandas dataframe

    Returns:
        current_csv: pandas dataframe which now has lat & lng - note this is not required as this is done 'inplace' but following the existing pattern
    """
    lat_lng_lists_dict = convert_link_to_lat_lng_lists(current_csv)
    print(lat_lng_lists_dict)

    current_csv["lat"] = lat_lng_lists_dict[
        "lat_list"
    ]  # this is enough to add columns - but lets follow the pattern so far & return reassign to variable
    current_csv["lng"] = lat_lng_lists_dict["lng_list"]

    return current_csv


def dataframe_to_json(current_csv, file_name):
    """save pandas dataframe to json

    Args:
        current_csv (pandas dataframe ): pandas dataframe
        file_name (string): file_name
    """
    print("saving json file" + file_name)
    current_csv.to_json(file_name + ".json", orient="records")


def split_address_to_coloumns(current_csv):
    """split the address into pincode & district and add columns for pincode & district in the dataframe

    Args:
        current_csv (dataframe): dataframe

    Returns:
        dataframe: dataframe
    """
    addresses_list = current_csv["address"].to_list()

    pincode_list = (
        []
    )  # this would the last item in split addresses & then need to get last 6 characters
    district_list = []  # 3rd last item

    for address in addresses_list:
        current_split_address = address.split(",")
        current_pincode = current_split_address[-1][-6:].strip()
        current_district = current_split_address[-3].strip()

        pincode_list.append(current_pincode)
        district_list.append(current_district)

    current_csv["pincode"] = pincode_list
    current_csv["district"] = district_list

    return current_csv


def remove_duplicate_rows_based_on_place_id(current_csv):
    """remove duplicates based on place_id column values

    Args:
        current_csv (dataframe): dataframe

    Returns:
        dataframe: duplicates_cleared_df
    """

    duplicates_cleared_df = current_csv.drop_duplicates(subset=["place_id"])

    return duplicates_cleared_df


def reviews_drop_columns(current_csv):
    """drops multiple columns in reviews file

    Args:
        current_csv (padas_dataframe): reviews file - pandas dataframe

    Returns:
        pandas dataframe: reviews pandas dataframe after removing unrequired columns
    """

    overview_drop_columns_list = [
        "response_from_owner_text",
        "response_from_owner_ago",
        "response_from_owner_date",
        "review_likes_count",
        "total_number_of_reviews_by_reviewer",
        "total_number_of_photos_by_reviewer",
        "is_local_guide",
        "review_translated_text",
        "response_from_owner_translated_text",
    ]

    return current_csv.drop(overview_drop_columns_list, axis=1)


def add_company_names(current_csv):
    """add company to a new column

    Args:
        current_csv (pandas_dataframe): pandas_dataframe

    Returns:
        pandas_dataframe: pandas_dataframe
    """

    company_name_list = ["Croma", "Reliance", "Vivek", "Vasanth"]

    name_column_list = current_csv["name"].to_list()

    company_names_for_column = []

    for name in name_column_list:
        smaller_cased_name = name.lower()

        for company in company_name_list:
            smaller_cased_company = company.lower()
            if smaller_cased_company in smaller_cased_name:
                company_names_for_column.append(company)
                break

    current_csv["company"] = company_names_for_column

    return current_csv


def convert_dates_add_columns(current_csv):

    current_csv["review_year"] = pd.DatetimeIndex(current_csv["published_at_date"]).year
    current_csv["review_month"] = pd.DatetimeIndex(
        current_csv["published_at_date"]
    ).strftime("%b")
    current_csv["review_month_year"] = pd.DatetimeIndex(
        current_csv["published_at_date"]
    ).strftime("%b-%Y")

    return current_csv


def process_overview_csv(current_csv, file_name):

    # drop unused columns
    current_csv = overview_drop_columns(current_csv)

    print(current_csv.columns)

    # remove by store names like xpress mini
    current_csv = overview_remove_by_store_names(current_csv)

    print(current_csv.shape)

    # remove based duplicates based on place_id - note - place_id is unique for each store
    current_csv = remove_duplicate_rows_based_on_place_id(current_csv)
    print(current_csv.shape)

    # extract latlng from url links & add seperate lat and lng columns to dataframe
    current_csv = add_lat_lng_columns_after_extracting_from_links(current_csv)

    print(current_csv)

    # split address into multiple columns
    # split address to get pincode & district - adds seperate columns for pincode & districts=
    current_csv = split_address_to_coloumns(current_csv)

    print(current_csv)

    # dropping address and link columns
    current_csv.drop(["address", "link"], axis=1, inplace=True)

    # add company to a new column
    current_csv = add_company_names(current_csv)

    # test saving to json
    dataframe_to_json(current_csv, file_name)

    current_csv.to_csv("test.csv", index=False)

    # try to acheive the file structure created in the todo list

    # make use of overview dataframe (need to store in higher scoped variable
    # then get values of place_id - select & group rows which contains the place_id -> then loop & group star rating to get stats
    # additionally group by year as well - get rows which either contains 2023 or 2024
    # for above items try pandas query method- already used in this file - refer for example

    # start processing detailed reviews file - use placeid as the unique identifier between two files

    # clean & process dataframe - remove unwated columns

    # need only 2023 & 2024 reviews - too much data can make things irrelavant - this range is recent & good

    # it good to mentioned scraped as of date - sep 09 2024 - have this noted in notes

    # process data with ai

    # rating star wise summary list of issue / success story - for each rating seperately

    # month wise product summary list - for each store

    return current_csv


def process_detailed_reviews_csv(current_csv, file_name):

    # drop unused columns
    current_csv = reviews_drop_columns(current_csv)

    print(current_csv.columns)

    current_csv = convert_dates_add_columns(current_csv)

    print(current_csv.columns)
    print(current_csv["review_month"][0])
    print(current_csv["review_year"][0])
    print(current_csv["review_month_year"][0])

    # os._exit(0)s

    return current_csv


def processe_csv_main(file_name):

    current_csv = read_csv(file_name)

    if file_name_dict["overview"] == file_name:

        print("overview file")
        print("overview file skipped - for processing please uncomment")
        # process the overview file
        return process_overview_csv(current_csv, file_name)

    else:

        print("detailed else statement")

    return process_detailed_reviews_csv(current_csv, file_name)


processed_overview_csv_dataframe = []  # overview csv dataframe will be stored

# {"croma":["placeid1","placeid2"],...}
buisness_name_to_placeid_list_dict = {}

# {"croma":{"placeid1":{"name":"croma","reviews":4.9,..}}}
overview_data_business_name_placeid_dict = {}

# {"croma":{"placeid1":{"name":"croma","reviews":4.9,..,"all_review_stats":{1_star:123,2_star:123,3_star:123,4_star:123,5_star:123},
# "review_stats_1year":{1_star:123,2_star:123,3_star:123,4_star:123,5_star:123},
# "bundled_reviews":{1_star:[list all one star reviews for this place id,2_star:[...],...]}
# }}}
overview_with_reviews_data_business_name_placeid_dict = {}


def generate_buisness_name_to_placeid_list_dict(processed_overview_csv_dataframe):
    """generate dictionaries in the requried format

    Args:
        processed_overview_csv_dataframe (dataframe): processed_overview_csv_dataframe

    Returns:
        buisness_name_to_placeid_list_dict, overview_data_business_name_placeid_dict: dictionaries
    """

    buisness_name_to_placeid_list_dict = {}
    overview_data_business_name_placeid_dict = {}

    company_name_list = ["Croma", "Reliance", "Vivek", "Vasanth"]

    for name in company_name_list:
        temp_list = [name]
        single_company_selected_dataframe = processed_overview_csv_dataframe.query(
            "company in @temp_list"
        )
        buisness_name_to_placeid_list_dict[name] = single_company_selected_dataframe[
            "place_id"
        ].to_list()

        dict_containing_df_as_records = single_company_selected_dataframe.to_dict(
            "records"
        )

        overview_data_business_name_placeid_dict[name] = {}

        for record in dict_containing_df_as_records:

            overview_data_business_name_placeid_dict[name][record["place_id"]] = record

    return buisness_name_to_placeid_list_dict, overview_data_business_name_placeid_dict


def generate_year_filtered_single_place_id_reviews_dataframe(
    single_place_id_single_rating_reviews_dataframe,
    include_sep_2023=False,
):
    drop_columns_list = [
        "place_id",
        "place_name",
        "review_id",
        "rating",
        "published_at",
        "published_at_date",
    ]
    cleaned_single_place_id_single_rating_reviews_dataframe = (
        single_place_id_single_rating_reviews_dataframe.drop(drop_columns_list, axis=1)
    )
    cleaned_single_place_id_single_rating_reviews_dataframe.dropna(
        subset=["review_text"], inplace=True
    )

    years = [2023, 2024]
    year_filtered_single_place_id_reviews_dataframe = (
        cleaned_single_place_id_single_rating_reviews_dataframe.query(
            "review_year in @years"
        )
    )

    if not include_sep_2023:

        year_month = ["Sep-2023"]
        year_filtered_single_place_id_reviews_dataframe = (
            year_filtered_single_place_id_reviews_dataframe.query(
                "review_month_year not in @year_month"
            )
        )

    return year_filtered_single_place_id_reviews_dataframe


def generate_overview_with_reviews_data_business_name_placeid_dict(
    processed_reviews_csv_dataframe,
    buisness_name_to_placeid_list_dict,
    overview_data_business_name_placeid_dict,
):

    deep_copy_overview_data_business_name_placeid_dict = copy.deepcopy(
        overview_data_business_name_placeid_dict
    )
    rating_dict = {
        1: "1_star",
        2: "2_star",
        3: "3_star",
        4: "4_star",
        5: "5_star",
    }

    for company_name in buisness_name_to_placeid_list_dict.keys():
        for place_id in buisness_name_to_placeid_list_dict[company_name]:

            deep_copy_overview_data_business_name_placeid_dict[company_name][place_id][
                "reviews_stats"
            ] = {
                "1_star": 0,
                "2_star": 0,
                "3_star": 0,
                "4_star": 0,
                "5_star": 0,
            }

            deep_copy_overview_data_business_name_placeid_dict[company_name][place_id][
                "bundled_reviews"
            ] = {
                "1_star": [],
                "2_star": [],
                "3_star": [],
                "4_star": [],
                "5_star": [],
            }

            temp_list = [place_id]
            single_place_id_reviews_dataframe = processed_reviews_csv_dataframe.query(
                "place_id in @temp_list"
            )
            for rating in rating_dict.keys():
                single_place_id_single_rating_reviews_dataframe = (
                    single_place_id_reviews_dataframe[
                        single_place_id_reviews_dataframe["rating"] == rating
                    ]
                )
                drop_columns_list = [
                    "place_id",
                    "place_name",
                    "review_id",
                    "rating",
                    "published_at",
                    "published_at_date",
                ]

                dict_containing_df_as_records = (
                    single_place_id_single_rating_reviews_dataframe.drop(
                        drop_columns_list, axis=1
                    ).to_dict("records")
                )

                deep_copy_overview_data_business_name_placeid_dict[company_name][
                    place_id
                ]["reviews_stats"][rating_dict[rating]] = len(
                    dict_containing_df_as_records
                )

                year_filtered_single_place_id_reviews_dataframe = (
                    generate_year_filtered_single_place_id_reviews_dataframe(
                        single_place_id_single_rating_reviews_dataframe,
                        include_sep_2023=True,
                    )
                )

                deep_copy_overview_data_business_name_placeid_dict[company_name][
                    place_id
                ]["bundled_reviews"][
                    rating_dict[rating]
                ] = year_filtered_single_place_id_reviews_dataframe[
                    "review_text"
                ].to_list()

            overview_data_business_name_placeid_dict[company_name][place_id][
                "reviews_stats"
            ] = deep_copy_overview_data_business_name_placeid_dict[company_name][
                place_id
            ][
                "reviews_stats"
            ]
            year_filtered_single_place_id_reviews_dataframe = (
                generate_year_filtered_single_place_id_reviews_dataframe(
                    single_place_id_single_rating_reviews_dataframe,
                    include_sep_2023=False,
                )
            )

            deep_copy_overview_data_business_name_placeid_dict[company_name][place_id][
                "month_split_reviews"
            ] = year_filtered_single_place_id_reviews_dataframe.groupby("review_month")["review_text"].apply(list).to_dict()

    return deep_copy_overview_data_business_name_placeid_dict


processed_overview_csv_dataframe = processe_csv_main(file_name_dict["overview"])


buisness_name_to_placeid_list_dict, overview_data_business_name_placeid_dict = (
    generate_buisness_name_to_placeid_list_dict(processed_overview_csv_dataframe)
)
print(buisness_name_to_placeid_list_dict, "bizz place id dict")


print(
    overview_data_business_name_placeid_dict, "overview_data_business_name_placeid_dict"
)

processed_reviews_csv_dataframe = processe_csv_main(file_name_dict["reviews"])

overview_with_reviews_data_business_name_placeid_dict = (
    generate_overview_with_reviews_data_business_name_placeid_dict(
        processed_reviews_csv_dataframe,
        buisness_name_to_placeid_list_dict,
        overview_data_business_name_placeid_dict,
    )
)

# print(overview_with_reviews_data_business_name_placeid_dict)
print(overview_with_reviews_data_business_name_placeid_dict.keys())

dict_with_names = {
    "buisness_name_to_placeid_list_dict": buisness_name_to_placeid_list_dict,
    "overview_data_business_name_placeid_dict": overview_data_business_name_placeid_dict,
    "overview_with_reviews_data_business_name_placeid_dict": overview_with_reviews_data_business_name_placeid_dict,
}

for dict_name in dict_with_names.keys():
    with open(dict_name + ".json", "w") as f:
        json.dump(dict_with_names[dict_name], f)
