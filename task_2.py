from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox") \
    .getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('./nuek-vuh3.csv')

# Розподіляємо дані
nuek_repart = nuek_df.repartition(2)

# Обробляємо дані
nuek_processed = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count()

# Проміжний action: collect
intermediate_results = nuek_processed.collect()  # Викликаємо collect() для отримання результатів

# Виводимо проміжні результати
print("Проміжні результати:")
for row in intermediate_results:
    print(row)

# Додатковий фільтр
nuek_processed = nuek_processed.where("count > 2")

# Отримуємо фінальні результати
final_results = nuek_processed.collect()  # Викликаємо collect() для фінальних результатів

# Виводимо фінальні результати
print("Фінальні результати:")
for row in final_results:
    print(row)

input("Press Enter to continue...")

# Закриваємо сесію Spark
spark.stop()
