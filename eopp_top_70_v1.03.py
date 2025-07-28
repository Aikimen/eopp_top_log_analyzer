"""
filename: eopp_top_50_v1.02.py
author: Eugene Arkhipov
Description: this module contains code that help to do
aggregation errors level:3 EOPP system from Graylog
and count top of errors.
"""


import os
import sys
import pandas as pd
import re

NUMBER_OF_ERRORS = 70


def normalize_message(message):
    # Функция для нормализации сообщений и извлечения переменных
    patterns = [
        (r'Ошибка при получении свободных слотов. МАПП: 1dae5b1c-e2b3-44a4-848f-df8ce2ddde42.*',
         'Ошибка при получении свободных слотов. МАПП: Забайкальск'),
        (r'Ошибка при получении свободных слотов. МАПП: cbde069a-7e18-4ca6-9b38-f790348d6c24.*',
         'Ошибка при получении свободных слотов. МАПП: Бугристое'),
        (r'Ошибка при получении свободных слотов. МАПП: ab6edb80-5f8f-4bf9-bf9a-a925271d9df8.*',
         'Ошибка при получении свободных слотов. МАПП: Чернышевское'),
        (r'Ошибка при получении свободных слотов. МАПП: 93c9939a-2182-4e78-98b4-0cf314b09cfa.*',
         'Ошибка при получении свободных слотов. МАПП: Тагиркент-Казмаляр'),
        # (r"Ошибка при получении свободных слотов\. МАПП: .*", 'Ошибка при получении свободных слотов. МАПП: ID'),
        (r'\b\w{8}-\w{4}-\w{4}-\w{4}-\w{12}\b', 'UUID'),
        (r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d+Z', 'TIMESTAMP'),
        (r'\d{2}\/\d{2}\/\d{4}', 'DATE'),
        (r'Пользователь: .*?', 'Пользователь: USER'),
        (r'id \d+', 'id ID'),
        (r'ExceptionMiddleware - Пользователь: .*', 'ExceptionMiddleware - Пользователь: USER'),

        (r"Ошибка при получении реестра слотов бронирования\. queryParams.*", 'Ошибка при получении реестра слотов бронирования.'),
        (r"Ошибка при получении агрегированных данных реестра слотов бронирования. queryParams.*", 'Ошибка при получении агрегированных данных реестра слотов бронирования.'),
        (r'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-notifications-email-sender-v2 .*',
         'R-FAULT eopp-notifications-email-sender-v2.'),
        ('R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-notify-event-v2 .*', 'R-FAULT eopp-notify-event-v2'),
        (r'Не удалось получить файл.*', 'Не удалось получить файл из хранилища.'),
        (r'Ошибка при отправке письма.*', 'Ошибка при отправке письма.'),
        (r'Ошибка создания транспортного средства. Детали: Недопустимый формат ГРН.*',
         'Ошибка создания транспортного средства. Детали: Недопустимый формат ГРН NUMBER'),
        (r'Запрос на перенос заявки по идентификатору ЕПГУ:.*',
         'Запрос на перенос заявки по идентификатору ЕПГУ: NUMBER завершился ошибкой.'),
        (r'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-unblocking-users.*',
         'R-FAULT eopp-unblocking-users'),
        (r'Health check redis with status Unhealthy.*',
         'Health check redis with status Unhealthy completed after XXXms with message'),
        (r'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-facility.*',
         'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-facility'),
        (r'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp_registration_transporter_request.*',
         'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp_registration_transporter_request'),
        (r'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-available-slots.*',
         'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-available-slots'),
        (r'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-available-dates.*',
         'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-available-dates'),
        (r'Health check npgsql with status Unhealthy.*', 'Health check npgsql with status Unhealthy'),
        (r'Health check healthcheck-redis with status Unhealthy.*',
         'Health check healthcheck-redis with status Unhealthy'),

        (r'.*An unhandled exception was thrown by the application',
         'An unhandled exception was thrown by the application'),
        (r'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-facilities-count-booking-days.*',
         'R-FAULT rabbitmq://rabbitmq.rabbitmq-production/eopp/eopp-get-facilities-count-booking-days'),

        # (r'Необработанная ошибка эндпоинта.*', 'Необработанная ошибка эндпоинта'),
        (r'Необработанная ошибка эндпоинта /v1/notification/unread-count',
         'Необработанная ошибка эндпоинта /v1/notification/unread-count'),
        (r'Необработанная ошибка эндпоинта /v1/timeslot/AvailableSlots',
         'Необработанная ошибка эндпоинта /v1/timeslot/AvailableSlots'),
        (r'Необработанная ошибка эндпоинта /Facility/Autocomplete',
         'Необработанная ошибка эндпоинта /Facility/Autocomplete'),
        (r'Необработанная ошибка эндпоинта /v1/Settings/VersionInfo',
         'Необработанная ошибка эндпоинта /v1/Settings/VersionInfo'),
        (r'Необработанная ошибка эндпоинта /v1/is-use-capcha', 'Необработанная ошибка эндпоинта /v1/is-use-capcha'),
        (r'Необработанная ошибка эндпоинта /v1/Search', 'Необработанная ошибка эндпоинта /v1/Search'),
        (r'Необработанная ошибка эндпоинта /v1/Handbook/available-countries',
         'Необработанная ошибка эндпоинта /v1/Handbook/available-countries'),
        (r'Необработанная ошибка эндпоинта /Account/GetCurrentUser',
         'Необработанная ошибка эндпоинта /Account/GetCurrentUser'),
        (r'Необработанная ошибка эндпоинта /v1/captcha', 'Необработанная ошибка эндпоинта /v1/captcha'),
        (r'Необработанная ошибка эндпоинта /v1/Handbook/TransportationTypes',
         'Необработанная ошибка эндпоинта /v1/Handbook/TransportationTypes'),
        (r'Необработанная ошибка эндпоинта /v1/timeslot/AvailableDates',
         'Необработанная ошибка эндпоинта /v1/timeslot/AvailableDates'),
        (r'Необработанная ошибка эндпоинта /v1/captcha-validate',
         'Необработанная ошибка эндпоинта /v1/captcha-validate'),
        (r'Необработанная ошибка эндпоинта /v1/Countries', 'Необработанная ошибка эндпоинта /v1/Countries'),
        (r'Необработанная ошибка эндпоинта /v1/APP', 'Необработанная ошибка эндпоинта /v1/APP'),
        (r'Необработанная ошибка эндпоинта /v1/save-country-and-facility',
         'Необработанная ошибка эндпоинта /v1/save-country-and-facility')
    ]

    for pattern, replacement in patterns:
        message = re.sub(pattern, replacement, message)

    return message


def normalize_exception(text):
    if pd.isna(text):
        return ''

    patterns = [
        (r'Npgsql.NpgsqlException (0x80004005): Unable to connect to a suitable host.*',
         'Npgsql.NpgsqlException (0x80004005): Unable to connect to a suitable host.'),
        (r'StackExchange.Redis.RedisTimeoutException: Timeout awaiting response.*',
         'Timeout к БД Redis'),
        (r'System.AggregateException: One or more errors occurred. (Unable to connect to a suitable host. Check inner exception for more details.).*',
         'Npgsql.NpgsqlException (0x80004005): Unable to connect to a suitable host.'),
#        (r'at System\..*?\n', 'at System.METHOD()\n'),
#      (r'at .*?\(.*?\)', 'at MODULE.METHOD(FILE)'),
#       (r'Exception: .*?\n', 'Exception: TYPE\n'),
#        (r'StackExchange\.Redis\..*?\n', 'StackExchange.Redis.EXCEPTION\n'),
#        (r'Message: .*', 'Message: ...'),
#        (r'---> .*?\n', '---> INNER_EXCEPTION\n'),
        (r'\[.*?\]', '[...]'),
        (r'\(.*?\)', '(...)'),
        (r'\s+в\s+строке\s+\d+', ' в строке N'),
        (r'id=\d+', 'id=ID'),
        (r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', 'UUID'),
    ]

    for pattern, repl in patterns:
        text = re.sub(pattern, repl, text, flags=re.MULTILINE)

    return text.strip()


# Определение текущей директории с использованием sys.executable
current_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))


# Проверка наличия файла raw_logs.csv
file_path = os.path.join(current_dir, 'raw_logs.csv')
if os.path.isfile(file_path):
    log_df = pd.read_csv(file_path)

    # Применение нормализации к каждому сообщению
    log_df['normalized_message'] = log_df['message'].apply(normalize_message)

    # Агрегация данных по уникальным сообщениям
    message_agg = log_df['normalized_message'].value_counts().reset_index()
    message_agg.columns = ['normalized_message', 'count']

    # Получаем топ-n ошибок
    top_errors = message_agg.head(NUMBER_OF_ERRORS)

    # Создание папки output и сохранение результата
    output_dir = os.path.join(current_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, f'top_{NUMBER_OF_ERRORS}_errors.csv')
    top_errors.to_csv(output_file_path, index=False)

    print(f"Топ-{NUMBER_OF_ERRORS} ошибок сохранен в файл: {output_file_path}")
else:
    print(f"Файл {file_path} не найден.")


# Агрегация по исключениям
if 'exception' in log_df.columns:
    log_df['normalized_exception'] = log_df['exception'].astype(str).apply(normalize_exception)

    exception_agg = log_df['normalized_exception'].value_counts().reset_index()
    exception_agg.columns = ['normalized_exception', 'count']
    top_exceptions = exception_agg.head(NUMBER_OF_ERRORS)

    exception_output_path = os.path.join(output_dir, f'top_{NUMBER_OF_ERRORS}_exceptions.csv')
    top_exceptions.to_csv(exception_output_path, index=False)

    print(f"Топ-{NUMBER_OF_ERRORS} исключений сохранён в файл: {exception_output_path}")
else:
    print("Колонка 'exception' не найдена в данных.")





# собираем .exe
# выполнить в terminal: pyinstaller --onefile file_name.py
