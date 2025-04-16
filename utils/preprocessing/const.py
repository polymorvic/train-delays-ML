MAIN_RAILWAY_STATIONS: list[str] = [
    'Wrocław Główny',
    'Kraków Główny',
    'Warszawa Centralna',
    'Szczecin Główny',
    'Rzeszów Główny',
    'Gdańsk Wrzeszcz',
    'Łódź Widzew',
    'Kołobrzeg',
    'Poznań Główny',
    'Katowice',
    'Białystok',
    'Częstochowa Stradom',
    'Lublin Główny',
    'Bydgoszcz Główna',
    'Olsztyn Główny',
    'Gdynia Główna',
    ]

MODELING_READY_COLNAMES_BLACKLIST: list[str] = [
    'id',
    'arrival_on_time',
    'departure_on_time',
    'station_count_on_curr_station',
    'full_route_station_count',
    'durations',
    'encoded_polylines',
    'decoded_polylines',
    'cumsum_distances',
    'distance_to_finish',
    'preciptype',
    'conditions',
    'icon',
    'in_poland',
    'nazwa_wojewodztwo',
    'id_wojewodztwo',
    'nazwa_powiat',
    'id_powiat',
    'nazwa_gmina',
    'id_gmina',
    ]

CATEGORY_COLUMNS: list[str] = [
    'relacja',
    'stacja',
    'train_type',
    'traction_type',
    'key',
    'prev_stations',
    'next_stations',
    ]

TARGETS_COL_ARRIVAL: str = 'opóźnienie przyjazdu'

TARGETS_COL_DEPARTURE: str = 'opóźnienie odjazdu'
