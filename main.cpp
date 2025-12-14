#include "/opt/homebrew/opt/postgresql@14/include/postgresql@14/libpq-fe.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <ctime>
#include <iomanip>
#include <cstring>

// Структуры данных
struct Product {
    int product_id;
    std::string product_name;
    std::string category;
    double price;
};

struct Customer {
    int customer_id;
    std::string customer_name;
    std::string region;
};

struct Sale {
    int sale_id;
    std::string sale_date;
    int product_id;
    int customer_id;
    int quantity;
    double amount;
};

// Функция подключения к PostgreSQL
PGconn* connectDB(const std::string& dbname = "dwh_db",
                  const std::string& user = "litichevskiyfedor",
                  const std::string& password = "Fedor28200791Fedor",
                  const std::string& host = "localhost",
                  int port = 5432) {

    std::string conninfo = "dbname=" + dbname + " user=" + user;
    if (!password.empty()) conninfo += " password=" + password;
    if (host != "localhost") conninfo += " host=" + host;
    if (port != 5432) conninfo += " port=" + std::to_string(port);

    PGconn* conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Ошибка подключения: " << PQerrorMessage(conn) << std::endl;
        PQfinish(conn);
        return nullptr;
    }

    std::cout << "✅ Подключено к PostgreSQL (база: " << dbname << ")" << std::endl;
    return conn;
}

// Функция выполнения SQL
void executeSQL(PGconn* conn, const std::string& sql, bool silent = false) {
    PGresult* res = PQexec(conn, sql.c_str());

    if (PQresultStatus(res) != PGRES_COMMAND_OK &&
        PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (!silent) {
            std::cerr << "SQL ошибка: " << PQerrorMessage(conn) << std::endl;
        }
    }

    PQclear(res);
}

// Функция чтения CSV файла
std::vector<std::vector<std::string>> readCSV(const std::string& filename) {
    std::vector<std::vector<std::string>> data;
    std::ifstream file(filename);

    if (!file.is_open()) {
        std::cerr << "❌ Не могу открыть файл: " << filename << std::endl;
        return data;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::vector<std::string> row;
        std::stringstream ss(line);
        std::string field;

        while (std::getline(ss, field, ',')) {
            row.push_back(field);
        }
        data.push_back(row);
    }

    file.close();
    return data;
}

// Функция загрузки данных из CSV в PostgreSQL
void loadDataFromCSV(PGconn* conn) {
    std::cout << "\n📥 Загрузка данных из CSV файлов..." << std::endl;

    // Очищаем таблицы перед загрузкой новых данных
    executeSQL(conn, "DELETE FROM sales_fact;", true);
    executeSQL(conn, "DELETE FROM time_dim;", true);
    executeSQL(conn, "DELETE FROM products_dim;", true);
    executeSQL(conn, "DELETE FROM customers_dim;", true);

    // 1. Загрузка продуктов
    auto products = readCSV("products.csv");
    if (!products.empty()) {
        std::cout << "Загрузка товаров..." << std::endl;
        int loaded = 0;
        for (size_t i = 1; i < products.size(); i++) { // Пропускаем заголовок
            if (products[i].size() >= 4) {
                // Экранируем кавычки в названиях
                std::string product_name = products[i][1];
                std::string category = products[i][2];

                // Убираем кавычки если есть
                if (!product_name.empty() && product_name.front() == '"' && product_name.back() == '"') {
                    product_name = product_name.substr(1, product_name.size() - 2);
                }
                if (!category.empty() && category.front() == '"' && category.back() == '"') {
                    category = category.substr(1, category.size() - 2);
                }

                // Заменяем одинарные кавычки на двойные для SQL
                size_t pos;
                while ((pos = product_name.find("'")) != std::string::npos) {
                    product_name.replace(pos, 1, "''");
                }
                while ((pos = category.find("'")) != std::string::npos) {
                    category.replace(pos, 1, "''");
                }

                std::string sql = "INSERT INTO products_dim (product_id, product_name, category, price) VALUES (" +
                                products[i][0] + ", '" + product_name + "', '" +
                                category + "', " + products[i][3] + ");";
                executeSQL(conn, sql, true);
                loaded++;
            }
        }
        std::cout << "✅ Загружено " << loaded << " товаров" << std::endl;
    }

    // 2. Загрузка клиентов
    auto customers = readCSV("customers.csv");
    if (!customers.empty()) {
        std::cout << "Загрузка клиентов..." << std::endl;
        int loaded = 0;
        for (size_t i = 1; i < customers.size(); i++) {
            if (customers[i].size() >= 3) {
                std::string customer_name = customers[i][1];
                std::string region = customers[i][2];

                // Убираем кавычки если есть
                if (!customer_name.empty() && customer_name.front() == '"' && customer_name.back() == '"') {
                    customer_name = customer_name.substr(1, customer_name.size() - 2);
                }
                if (!region.empty() && region.front() == '"' && region.back() == '"') {
                    region = region.substr(1, region.size() - 2);
                }

                // Экранируем кавычки
                size_t pos;
                while ((pos = customer_name.find("'")) != std::string::npos) {
                    customer_name.replace(pos, 1, "''");
                }
                while ((pos = region.find("'")) != std::string::npos) {
                    region.replace(pos, 1, "''");
                }

                std::string sql = "INSERT INTO customers_dim (customer_id, customer_name, region) VALUES (" +
                                customers[i][0] + ", '" + customer_name + "', '" +
                                region + "');";
                executeSQL(conn, sql, true);
                loaded++;
            }
        }
        std::cout << "✅ Загружено " << loaded << " клиентов" << std::endl;
    }

    // 3. Загрузка продаж
    auto sales = readCSV("sales.csv");
    if (!sales.empty()) {
        std::cout << "Загрузка продаж..." << std::endl;
        int loaded = 0;
        for (size_t i = 1; i < sales.size(); i++) {
            if (sales[i].size() >= 6) {
                std::string sql = "INSERT INTO sales_fact (sale_id, sale_date, product_id, customer_id, quantity, amount) VALUES (" +
                                sales[i][0] + ", '" + sales[i][1] + "', " +
                                sales[i][2] + ", " + sales[i][3] + ", " +
                                sales[i][4] + ", " + sales[i][5] + ");";
                executeSQL(conn, sql, true);
                loaded++;
            }
        }
        std::cout << "✅ Загружено " << loaded << " продаж" << std::endl;
    }
}

// Функция для создания индексов
void createIndexes(PGconn* conn) {
    std::cout << "\n⚡ Создание индексов для оптимизации запросов..." << std::endl;

    // Удаляем индексы если они уже существуют (тихо, без ошибок)
    executeSQL(conn, "DROP INDEX IF EXISTS idx_sales_fact_product_id;", true);
    executeSQL(conn, "DROP INDEX IF EXISTS idx_sales_fact_customer_id;", true);
    executeSQL(conn, "DROP INDEX IF EXISTS idx_sales_fact_sale_date;", true);
    executeSQL(conn, "DROP INDEX IF EXISTS idx_products_dim_category;", true);
    executeSQL(conn, "DROP INDEX IF EXISTS idx_customers_dim_region;", true);

    // Создаем индексы
    executeSQL(conn, "CREATE INDEX idx_sales_fact_product_id ON sales_fact(product_id);");
    executeSQL(conn, "CREATE INDEX idx_sales_fact_customer_id ON sales_fact(customer_id);");
    executeSQL(conn, "CREATE INDEX idx_sales_fact_sale_date ON sales_fact(sale_date);");
    executeSQL(conn, "CREATE INDEX idx_products_dim_category ON products_dim(category);");
    executeSQL(conn, "CREATE INDEX idx_customers_dim_region ON customers_dim(region);");

    std::cout << "✅ Индексы созданы" << std::endl;
}

// Функция выполнения аналитических запросов
void runAnalyticalQueries(PGconn* conn) {
    std::cout << "\n📊 ВЫПОЛНЕНИЕ АНАЛИТИЧЕСКИХ ЗАПРОСОВ" << std::endl;
    std::cout << "===================================" << std::endl;

    // Запрос 1: Объем продаж по категориям товаров
    std::cout << "\n1. Объем продаж по категориям товаров:" << std::endl;
    std::cout << "-----------------------------------" << std::endl;

    const char* query1 =
        "SELECT p.category, ROUND(SUM(s.amount)::numeric, 2) AS total_sales "
        "FROM sales_fact s "
        "JOIN products_dim p ON s.product_id = p.product_id "
        "GROUP BY p.category "
        "ORDER BY total_sales DESC;";

    PGresult* res1 = PQexec(conn, query1);
    if (PQresultStatus(res1) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res1); i++) {
            std::cout << "   " << PQgetvalue(res1, i, 0)
                     << ": $" << PQgetvalue(res1, i, 1) << std::endl;
        }
    }
    PQclear(res1);

    // Запрос 2: Количество покупок по регионам
    std::cout << "\n2. Количество покупок по регионам:" << std::endl;
    std::cout << "-----------------------------------" << std::endl;

    const char* query2 =
        "SELECT c.region, COUNT(s.sale_id) AS number_of_sales "
        "FROM sales_fact s "
        "JOIN customers_dim c ON s.customer_id = c.customer_id "
        "GROUP BY c.region "
        "ORDER BY number_of_sales DESC;";

    PGresult* res2 = PQexec(conn, query2);
    if (PQresultStatus(res2) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res2); i++) {
            std::cout << "   " << PQgetvalue(res2, i, 0)
                     << ": " << PQgetvalue(res2, i, 1) << " покупок" << std::endl;
        }
    }
    PQclear(res2);

    // Запрос 3: Средний чек по месяцам
    std::cout << "\n3. Средний чек по месяцам:" << std::endl;
    std::cout << "---------------------------" << std::endl;

    const char* query3 =
        "SELECT EXTRACT(YEAR FROM s.sale_date) as year, "
        "EXTRACT(MONTH FROM s.sale_date) as month, "
        "ROUND(AVG(s.amount)::numeric, 2) AS average_ticket "
        "FROM sales_fact s "
        "GROUP BY EXTRACT(YEAR FROM s.sale_date), EXTRACT(MONTH FROM s.sale_date) "
        "ORDER BY year, month;";

    PGresult* res3 = PQexec(conn, query3);
    if (PQresultStatus(res3) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res3); i++) {
            // Форматируем вывод месяца с ведущим нулем
            std::string month = PQgetvalue(res3, i, 1);
            if (std::stoi(month) < 10) {
                month = "0" + month;
            }
            std::cout << "   " << PQgetvalue(res3, i, 0) << "-"
                     << month << ": $"
                     << PQgetvalue(res3, i, 2) << std::endl;
        }
    }
    PQclear(res3);

    // Дополнительный запрос: Топ-3 самых продаваемых товара
    std::cout << "\n4. Топ-3 самых продаваемых товара:" << std::endl;
    std::cout << "-----------------------------------" << std::endl;

    const char* query4 =
        "SELECT p.product_name, SUM(s.quantity) as total_quantity, ROUND(SUM(s.amount)::numeric, 2) as total_amount "
        "FROM sales_fact s "
        "JOIN products_dim p ON s.product_id = p.product_id "
        "GROUP BY p.product_name "
        "ORDER BY total_amount DESC "
        "LIMIT 3;";

    PGresult* res4 = PQexec(conn, query4);
    if (PQresultStatus(res4) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res4); i++) {
            std::cout << "   " << (i+1) << ". " << PQgetvalue(res4, i, 0)
                     << " - " << PQgetvalue(res4, i, 1) << " шт. ($"
                     << PQgetvalue(res4, i, 2) << ")" << std::endl;
        }
    }
    PQclear(res4);
}

// Основная функция
int main() {
    std::cout << "=========================================" << std::endl;
    std::cout << "   Data Warehouse - Загрузка данных" << std::endl;
    std::cout << "   (Таблицы уже созданы в PostgreSQL)" << std::endl;
    std::cout << "=========================================" << std::endl;

    // Подключение к PostgreSQL
    PGconn* conn = connectDB();
    if (!conn) {
        std::cerr << "\n❌ Не удалось подключиться к PostgreSQL" << std::endl;
        std::cerr << "Убедитесь что:" << std::endl;
        std::cerr << "1. PostgreSQL запущен: brew services start postgresql@14" << std::endl;
        std::cerr << "2. База данных 'dwh_db' существует" << std::endl;
        std::cerr << "3. Таблицы созданы (см. структуру ниже)" << std::endl;
        return 1;
    }

    // Загрузка данных из CSV файлов
    loadDataFromCSV(conn);

    // Создание индексов (пропустите если индексы уже созданы)
    createIndexes(conn);

    // Выполнение аналитических запросов
    runAnalyticalQueries(conn);

    // Закрытие соединения
    PQfinish(conn);

    std::cout << "\n=========================================" << std::endl;
    std::cout << "✅ Программа успешно выполнена!" << std::endl;
    std::cout << "Данные загружены в PostgreSQL." << std::endl;
    std::cout << "Аналитические запросы выполнены." << std::endl;
    std::cout << "=========================================" << std::endl;

    return 0;
}