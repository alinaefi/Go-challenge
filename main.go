// Алина Ефимова
// Тестовое задание
// Для запуска введите $go run main.go {id заказов через запятую без пробелов}
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// класс ответственный за отправление запросов в базу данных
type OrderFetcher struct {
	DB *sql.DB
}

// конструктор класса OrderFetcher
func NewOrderFetcher(db *sql.DB) *OrderFetcher {
	return &OrderFetcher{DB: db}
}

// класс ответственный за вывод данных
type ResultPrinter struct {
	requiredItems [][]string
}

// конструктор класса ResultPrinter
func NewResultPrinter(requiredItems [][]string) *ResultPrinter {
	return &ResultPrinter{requiredItems: requiredItems}
}

// функция выполняет запрос в бд и возвращает выборку данных о продуктах в заказе
// в формате [[itemId, orderId, itemQuantity], [...]]
func (of *OrderFetcher) fetchRequiredItems(orderIds []string) ([][]string, error) {
	// валидация параметров
	if len(orderIds) < 1 {
		return nil, fmt.Errorf("необходимо ввести хоть один параметр")
	}
	query := `SELECT order_id, item_id, quantity FROM order_items WHERE order_id IN (` + strings.Repeat("?,", len(orderIds)-1) + `?) ORDER BY item_id;`

	// создание структуры данных
	var requiredItems [][]string

	// подготовка запроса
	q, err := of.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	args := make([]interface{}, len(orderIds))
	for i, v := range orderIds {
		args[i] = v
	}

	// отправка запроса в бд
	rows, err := q.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// обработка результатов
	for rows.Next() {
		var orderId, itemId, itemQuantity string
		err := rows.Scan(&orderId, &itemId, &itemQuantity)
		if err != nil {
			return nil, err
		}
		requiredItems = append(requiredItems, []string{itemId, orderId, itemQuantity})
	}

	// проверка на наличие ошибок при обработке результатов
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return requiredItems, nil
}

// функция выполняет запрос в бд, дополняет существующую выборку новыми данными и
// возвращает выборку данных о продуктах в заказе
// в формате [[itemId, orderId, itemQuantity, itemName], [...]]
func (of *OrderFetcher) fetchItemNames(itemIds []string, requiredItems [][]string) ([][]string, error) {
	// валидация параметров
	if len(itemIds) < 1 {
		return nil, fmt.Errorf("необходимо ввести хоть один параметр")
	}
	// подготовка запроса
	query1 := `SELECT id, name FROM items WHERE id IN (` + strings.Repeat("?,", len(itemIds)-1) + `?);`

	// создание структуры данных для временного хранения результатов
	itemNameMap := make(map[string]string)

	// подготовка параметров
	q, err := of.DB.Prepare(query1)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	args := make([]interface{}, len(itemIds))
	for i, v := range itemIds {
		args[i] = v
	}

	// отправка запроса в бд
	rows, err := q.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// обработка результатов запроса
	for rows.Next() {
		var itemId, itemName string
		err := rows.Scan(&itemId, &itemName)
		if err != nil {
			return nil, err
		}
		_, exists := itemNameMap[itemId]
		if !exists {
			itemNameMap[itemId] = itemName
		}
	}
	// добавление данных о названии продукта
	for i, values := range requiredItems {
		if itemName, exists := itemNameMap[values[0]]; exists {
			requiredItems[i] = append(values, itemName)
		}
	}
	return requiredItems, nil
}

// функция выполняет запрос в бд, дополняет существующую выборку новыми данными и
// возвращает выборку данных о продуктах в заказе
// в формате [[itemId, orderId, itemQuantity, itemName, itemSubRack1 itemSubRack2..., itemMainRack], [...]]
func (of *OrderFetcher) fetchItemsRacks(itemIds []string, requiredItems [][]string) ([][]string, error) {
	// валидация параметров
	if len(itemIds) < 1 {
		return nil, fmt.Errorf("необходимо ввести хоть один параметр")
	}

	// создание структуры данных для временного хранения результатов
	itemSubRackMap := make(map[string]string)

	// подготовка первого запроса (доп стеллажи)
	query1 := `SELECT rack_id, item_id, rack_name FROM item_rack WHERE is_main = FALSE AND item_id IN (` + strings.Repeat("?,", len(itemIds)-1) + `?) ORDER BY rack_id;`

	q, err := of.DB.Prepare(query1)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	defer q.Close()
	args := make([]interface{}, len(itemIds))
	for i, v := range itemIds {
		args[i] = v
	}

	// отправка запроса в бд
	rows, err := q.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// обработка результатов запроса
	for rows.Next() {
		var itemId, rackId, rackName string
		err := rows.Scan(&rackId, &itemId, &rackName)
		if err != nil {
			return nil, err
		}
		if existingRack, exists := itemSubRackMap[itemId]; exists {
			itemSubRackMap[itemId] = existingRack + "," + rackName
		} else {
			itemSubRackMap[itemId] = rackName
		}
	}

	// добавление данных о названиях дополнительных стеллажей
	for i, values := range requiredItems {
		// если доп стеллажей нет, место займет "0"
		requiredItems[i] = append(values, "0")
		if rackName, exists := itemSubRackMap[values[0]]; exists {
			requiredItems[i] = append(values, rackName)
		}
	}

	// подготовка второго запроса (главные стеллажи)
	query2 := `SELECT rack_id, item_id, rack_name FROM item_rack WHERE is_main = TRUE AND item_id IN (` + strings.Repeat("?,", len(itemIds)-1) + `?) ORDER BY rack_name;`

	// создание структуры данных для временного хранения результатов
	itemRackMap := make(map[string]string)

	// подготовка запроса
	q, err = of.DB.Prepare(query2)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	defer q.Close()
	for i, v := range itemIds {
		args[i] = v
	}

	// отправка запроса в бд
	rows, err = q.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// обработка результатов запроса
	for rows.Next() {
		var itemId, rackId, rackName string
		err := rows.Scan(&rackId, &itemId, &rackName)
		if err != nil {
			return nil, err
		}
		if existingRack, exists := itemRackMap[itemId]; exists {
			itemRackMap[itemId] = existingRack + "," + rackName
		} else {
			itemRackMap[itemId] = rackName
		}
	}
	// добавление данных о названиях главных стеллажей
	for i, values := range requiredItems {
		if rackName, exists := itemRackMap[values[0]]; exists {
			requiredItems[i] = append(values, rackName)
		}
	}
	return requiredItems, nil
}

// функция печатает конечный результат на основе структуры данных в формате
// [[itemId, orderId, itemQuantity, itemName, itemSubRack1 itemSubRack2..., itemMainRack], [...]]
func (of *ResultPrinter) printResult() error {
	currRack := ""
	for _, values := range of.requiredItems {
		if currRack != values[len(values)-1] {
			fmt.Printf("===== Стеллаж %v:\n", values[len(values)-1])
		}
		currRack = values[len(values)-1]
		fmt.Printf("%v (id=%v)\n", values[3], values[0])
		fmt.Printf("заказ %v, %v шт\n", values[1], values[2])
		if values[4] != "0" {
			fmt.Printf("доп. стеллаж: %v\n", values[4])
		}
		println()
	}
	return nil
}

func main() {
	// валидация параметров командной строки
	if len(os.Args) < 2 {
		fmt.Println("Перечислите параметры через запятую без пробелов")
		return
	}

	// парсинг параметров командной строки
	orderIds := strings.Split(os.Args[1], ",")

	// соединение с базой данных
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/go_challenge")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// проверка соединения с базой данных
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// создание объекта класса orderFetcher
	orderFetcher := NewOrderFetcher(db)

	// вызов метода FetchRequiredItems для выполнения запроса и хранения результатов
	requiredItems, err := orderFetcher.fetchRequiredItems(orderIds)
	if err != nil {
		log.Fatal(err)
	}

	// вызов метода FetchItemNames для выполнения запроса и дополнения структуры данных
	// подготовка параметров
	var itemIDs []string
	for _, values := range requiredItems {
		if len(values) > 0 {
			itemIDs = append(itemIDs, values[0])
		}
	}
	requiredItems, err = orderFetcher.fetchItemNames(itemIDs, requiredItems)
	if err != nil {
		log.Fatal(err)
	}

	// вызов метода FetchItemRacks для выполнения запроса и дополнения структуры данных
	requiredItems, err = orderFetcher.fetchItemsRacks(itemIDs, requiredItems)
	if err != nil {
		log.Fatal(err)
	}

	// начало вывода
	// создание объекта класса ResultPrinter
	resultPrinter := NewResultPrinter(requiredItems)

	// вывод заголовка
	fmt.Printf("=+=+=+=\nСтраница сборки заказов %s\n", os.Args[1])
	// вывод данных
	err = resultPrinter.printResult()
	if err != nil {
		log.Fatal(err)
	}
}
