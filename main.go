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

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

// класс ответственный за отправление запроса о заказах
type OrderFetcher struct {
	DB *sql.DB
}

// конструктор класса OrderFetcher
func NewOrderFetcher(db *sql.DB) *OrderFetcher {
	return &OrderFetcher{DB: db}
}

// класс для хранения информации о заказанном продукте
type OrderedItem struct {
	OrderID      int
	ItemQuantity int
}

// конструктор класса OrderedItem
func NewOrderedItem(orderID, itemQuantity int) *OrderedItem {
	return &OrderedItem{
		OrderID:      orderID,
		ItemQuantity: itemQuantity,
	}
}

// класс для хранения информации о главном стеллаже
type MainRack struct {
	RackName string
	ItemIDs  []int
}

// конструктор класса MainRack
func NewMainRack(rackName string, itemIDs []int) *MainRack {
	return &MainRack{
		RackName: rackName,
		ItemIDs:  itemIDs,
	}
}

// функция выполняет запрос в бд и возвращает выборку данных о продуктах в заказе
// в формате map[itemId]: [OrderedItem1, OrderedItem2, ...]
func (of *OrderFetcher) fetchRequiredItems(orderIds []string) (map[int][]OrderedItem, error) {
	// валидация параметров
	if len(orderIds) < 1 {
		return nil, fmt.Errorf("необходимо ввести хоть один параметр")
	}

	query, args, err := sqlx.In("SELECT order_id, item_id, quantity FROM order_items WHERE order_id IN (?)", orderIds)
	if err != nil {
		return nil, err
	}
	// создание структуры данных для хранения результатов
	orderedItemsMap := make(map[int][]OrderedItem)

	q, err := of.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// отправка запроса в бд
	rows, err := q.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// обработка результатов запроса
	for rows.Next() {
		var orderId, itemId, itemQuantity int
		err := rows.Scan(&orderId, &itemId, &itemQuantity)
		if err != nil {
			return nil, err
		}

		var data []OrderedItem

		existingData, exists := orderedItemsMap[itemId]
		if !exists {
			// если объект еще не в мапе, создаем новый объект класса OrderedItem
			data = []OrderedItem{}
		} else {
			data = existingData
		}

		newOrderedItem := NewOrderedItem(orderId, itemQuantity)
		data = append(data, *newOrderedItem)

		orderedItemsMap[itemId] = data
	}

	// проверка на наличие ошибок при обработке результатов
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return orderedItemsMap, nil
}

// функция выполняет запрос в бд и возвращает выборку данных о продуктах в заказе
// в формате map[itemId]: itemName
func (of *OrderFetcher) fetchItemName(itemIds []int) (map[int]string, error) {
	// валидация параметров
	if len(itemIds) < 1 {
		return nil, fmt.Errorf("необходимо ввести хоть один параметр")
	}

	query, args, err := sqlx.In("SELECT id, name FROM items WHERE id IN (?)", itemIds)
	if err != nil {
		return nil, err
	}
	// создание структуры данных для хранения результатов
	itemInfoMap := make(map[int]string)

	q, err := of.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// отправка запроса в бд
	rows, err := q.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// обработка результатов
	for rows.Next() {
		var itemId int
		var itemName string
		err := rows.Scan(&itemId, &itemName)
		if err != nil {
			return nil, err
		}
		_, exists := itemInfoMap[itemId]
		if !exists {
			itemInfoMap[itemId] = itemName
		}
	}
	// проверка на наличие ошибок при обработке результатов
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return itemInfoMap, nil
}

// функция выполняет запрос в бд и возвращает две структуры данных с результатами:
// 1) map[rackId]:MainRack1 для хранения результатов о главных стеллажах
// 2) map[itemId]:subRackName1, subRackName2...(concatenated string) для
// хранения результатов о доп стеллажах
func (of *OrderFetcher) fetchRackInfo(itemIds []int) (map[int]MainRack, map[int]string, error) {
	// валидация параметров
	if len(itemIds) < 1 {
		return nil, nil, fmt.Errorf("необходимо ввести хоть один параметр")
	}

	query, args, err := sqlx.In("SELECT rack_id, rack_name, item_id, is_main FROM item_rack WHERE item_id IN (?)", itemIds)
	if err != nil {
		return nil, nil, err
	}

	// создание структур данных для хранения результатов
	mainRackItemMap := make(map[int]MainRack)
	subRackNameMap := make(map[int]string)

	q, err := of.DB.Prepare(query)
	if err != nil {
		return nil, nil, err
	}
	defer q.Close()

	// отправка запроса в бд
	rows, err := q.Query(args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rackId, itemId int
		var rackName string
		var isMain bool
		err := rows.Scan(&rackId, &rackName, &itemId, &isMain)
		if err != nil {
			return nil, nil, err
		}
		// если главный стеллаж, сохраняем в одну структуру данных
		if isMain {
			existingData, exists := mainRackItemMap[rackId]
			if !exists {
				existingData = MainRack{
					RackName: rackName,
					ItemIDs:  []int{itemId},
				}
			} else {
				existingData.ItemIDs = append(existingData.ItemIDs, itemId)
			}

			mainRackItemMap[rackId] = existingData
		} else {
			// доп стеллажи связываем с itemId для удобства вывода
			existingValue, exists := subRackNameMap[itemId]
			if exists {
				subRackNameMap[itemId] = existingValue + "," + rackName
			} else {
				subRackNameMap[itemId] = rackName
			}
		}
	}
	// проверка на наличие ошибок при обработке результатов
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return mainRackItemMap, subRackNameMap, nil
}

func main() {
	// валидация параметров командной строки
	if len(os.Args) < 2 {
		fmt.Println("Перечислите параметры через запятую без пробелов")
		return
	}

	// парсинг параметров командной строки
	orderIds := strings.Split(os.Args[1], ",")

	// сокдинение с базой данных
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

	// создание объекта класса OrderFetcher
	orderFetcher := NewOrderFetcher(db)

	// вызов метода fetchRequiredItems для выполнения запроса и хранения результатов
	orderedItemsMap, err := orderFetcher.fetchRequiredItems(orderIds)
	if err != nil {
		log.Fatal(err)
	}

	// вызов метода fetchItemName для выполнения запроса и хранения результатов
	// подготовка параметров
	var itemIds []int

	for key := range orderedItemsMap {
		itemIds = append(itemIds, key)
	}
	itemNameMap, err := orderFetcher.fetchItemName(itemIds)
	if err != nil {
		log.Fatal(err)
	}
	// вызов метода fetchRackInfo для выполнения запроса и хранения результатов
	mainRackItemMap, subRackNameMap, err := orderFetcher.fetchRackInfo(itemIds)
	if err != nil {
		log.Fatal(err)
	}

	// начало вывода
	fmt.Printf("=+=+=+= Страница сборки заказов %s\n", os.Args[1])
	for _, mainRack := range mainRackItemMap {
		fmt.Printf("=====Стеллаж %v:\n", mainRack.RackName)
		for _, itemId := range mainRack.ItemIDs {
			itemName := itemNameMap[itemId]
			for _, orderedItem := range orderedItemsMap[itemId] {
				itemInOrder := orderedItem.OrderID
				itemQuantityInOrder := orderedItem.ItemQuantity
				itemSubRack, exists := subRackNameMap[itemId]
				fmt.Printf("%v (id=%v)\nзаказ %v, %v шт \n", itemName, itemId, itemInOrder, itemQuantityInOrder)
				if exists {
					fmt.Printf("доп стеллаж: %v\n", itemSubRack)
				}
				fmt.Println()
			}
		}
	}
}
