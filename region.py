import psycopg2
import spacy

# Загрузка модели spaCy для русского языка
nlp = spacy.load("ru_core_news_sm")

# Параметры подключения к базе данных
conn = psycopg2.connect(
    host="10.28.51.4",
    port="5433",
    database="postgres",
    user="hackathon-user-02",
    password="P2o3PqRs4T"
)

# Создание курсора для выполнения SQL запросов
cur = conn.cursor()

# Запрос данных о названии видео и его описании из базы данных
cur.execute("SELECT video_name, description FROM video")
data = cur.fetchall()


# Функция для извлечения регионов из текста с помощью NER
def extract_regions(text):
    doc = nlp(text)
    regions = []
    for ent in doc.ents:
        if ent.label_ == "GPE":  # GPE - General Purpose Entity (регион)
            regions.append(ent.text)
    return regions


# Сопоставление найденных регионов с вашим списком регионов
regions_list = []  # ваш список регионов
for line in 'regions.txt':
    regions_list.append(line)


for row in data:
    title = row[0]
    description = row[1]

    all_text = title + " " + description
    found_regions = extract_regions(all_text)

    matched_regions = set(found_regions).intersection(regions_list)

    if matched_regions:
        print(f"Найденные регионы для видео '{title}': {matched_regions}")
    else:
        print(f"Для видео '{title}' не найдены соответствующие регионы.")

# Закрытие курсора и соединения с базой данных
cur.close()
conn.close()