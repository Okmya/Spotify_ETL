def check_date(data_list):
    for album in data_list:
        date_parts = album["release_date"].split("-")

        # check if year is correct
        if len(date_parts) == 1:  # only year in data
            album["release_date"] += "-01-01"
        elif len(date_parts) == 2:  # year and month in data
            album["release_date"] += "-01"

    return data_list
