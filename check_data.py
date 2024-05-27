def check_date(data_list):
    for album in data_list:
        date_parts = album["release_date"].split("-")

        # sprawdzamy dlugosc daty
        if len(date_parts) == 1:  # tylko rok
            album["release_date"] += "-01-01"
        elif len(date_parts) == 2:  # rok i miesiąc
            album["release_date"] += "-01"

    return data_list
