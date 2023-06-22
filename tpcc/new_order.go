package tpcc

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	newOrderSelectCustomer = `SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?`
	newOrderSelectDistrict = `SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE`
	newOrderUpdateDistrict = `UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?`
	newOrderInsertOrder    = `INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)`
	newOrderInsertNewOrder = `INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)`
	newOrderUpdateStock    = `UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + ? WHERE s_i_id = ? AND s_w_id = ?`
)

var (
	newOrderSelectItemSQLs      [16]string
	newOrderSelectStockSQLs     [16]string
	newOrderInsertOrderLineSQLs [16]string
)

func init() {
	for i := 5; i <= 15; i++ {
		newOrderSelectItemSQLs[i] = genNewOrderSelectItemsSQL(i)
		newOrderSelectStockSQLs[i] = genNewOrderSelectStockSQL(i)
		newOrderInsertOrderLineSQLs[i] = genNewOrderInsertOrderLineSQL(i)
	}
}

func genNewOrderSelectItemsSQL(cnt int) string {
	buf := bytes.NewBufferString("SELECT i_price, i_name, i_data, i_id FROM item WHERE i_id IN (")
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('?')
	}
	buf.WriteByte(')')
	return buf.String()
}

func genNewOrderSelectStockSQL(cnt int) string {
	buf := bytes.NewBufferString("SELECT s_i_id, s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE (s_w_id, s_i_id) IN (")
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("(?,?)")
	}
	buf.WriteString(") FOR UPDATE")
	return buf.String()
}

func genNewOrderInsertOrderLineSQL(cnt int) string {
	buf := bytes.NewBufferString("INSERT into order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES ")
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("(?,?,?,?,?,?,?,?,?)")
	}
	return buf.String()
}

func (w *Workloader) otherWarehouse(ctx context.Context, warehouse int) int {
	s := getTPCCState(ctx)

	if w.cfg.Warehouses == 1 {
		return warehouse
	}

	var other int
	for {
		other = randInt(s.R, 1, w.cfg.Warehouses)
		if other != warehouse {
			break
		}
	}
	return other
}

type orderItem struct {
	OlSupplyWID int
	OlIID       int
	OlNumber    int
	OlQuantity  int
	OlAmount    float64
	OlDeliveryD sql.NullString

	IPrice float64
	IName  string
	IData  string

	FoundInItems    bool
	FoundInStock    bool
	SQuantity       int
	SDist           string
	RemoteWarehouse int
}

type newOrderData struct {
	WID    int
	DID    int
	CID    int
	OOlCnt int

	CDiscount float64
	CLast     string
	CCredit   []byte
	WTax      float64

	DNextOID int
	DTax     float64
}

type newOrderParameters struct {
	Data     newOrderData
	AllLocal int
	Items    []orderItem
	ItemsMap map[int]*orderItem
}

type NewOrderTxn struct {
	Params newOrderParameters
	Name   string
}

func (w *Workloader) runNewOrder(ctx context.Context, thread int, params newOrderParameters) error {
	s := getTPCCState(ctx)
	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	Data := params.Data
	Items := params.Items
	AllLocal := params.AllLocal
	//itemsMap := params.ItemsMap
	itemsMap := make(map[int]*orderItem, Data.OOlCnt)
	for i := 0; i < len(Items); i++ {
		item := &Items[i]
		itemsMap[item.OlIID] = item
	}
	//for _, it := range Items {
	//	//p := &it
	//	fmt.Println(it)
	//	fmt.Printf("address of slice %p\n", &it)
	//	itemsMap[it.OlIID] = &it
	//	p := itemsMap[it.OlIID]
	//	fmt.Printf("address of slice %p\n", p)
	//	p.FoundInItems = true
	//	fmt.Println(itemsMap[it.OlIID])
	//}
	//itemsMap[Items[0].OlIID].FoundInItems = true

	// TODO: support prepare statement

	// Process 1
	if err := s.newOrderStmts[newOrderSelectCustomer].QueryRowContext(ctx, Data.WID, Data.DID, Data.CID).Scan(&Data.CDiscount, &Data.CLast, &Data.CCredit, &Data.WTax); err != nil {
		return fmt.Errorf("exec %s(wID=%d,dID=%d,cID=%d) failed %v", newOrderSelectCustomer, Data.WID, Data.DID, Data.CID, err)
	}

	// Process 2
	if err := s.newOrderStmts[newOrderSelectDistrict].QueryRowContext(ctx, Data.DID, Data.WID).Scan(&Data.DNextOID, &Data.DTax); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderSelectDistrict, err)
	}

	// Process 3
	if _, err := s.newOrderStmts[newOrderUpdateDistrict].ExecContext(ctx, Data.DNextOID, Data.DID, Data.WID); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderUpdateDistrict, err)
	}

	OID := Data.DNextOID

	// Process 4
	if _, err := s.newOrderStmts[newOrderInsertOrder].ExecContext(ctx, OID, Data.DID, Data.WID, Data.CID,
		time.Now().Format(timeFormat), Data.OOlCnt, AllLocal); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderInsertOrder, err)
	}

	// Process 5

	// INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (:o_id , :Data _id , :w _id );
	// query = `INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)`
	if _, err := s.newOrderStmts[newOrderInsertNewOrder].ExecContext(ctx, OID, Data.DID, Data.WID); err != nil {
		return fmt.Errorf("exec %s failed %v", newOrderInsertNewOrder, err)
	}

	// Process 6
	selectItemSQL := newOrderSelectItemSQLs[len(Items)]
	selectItemArgs := make([]interface{}, len(Items))
	for i := range Items {
		selectItemArgs[i] = Items[i].OlIID
	}
	rows, err := s.newOrderStmts[selectItemSQL].QueryContext(ctx, selectItemArgs...)
	if err != nil {
		return fmt.Errorf("exec %s failed %v", selectItemSQL, err)
	}
	for rows.Next() {
		var tmpItem orderItem
		err := rows.Scan(&tmpItem.IPrice, &tmpItem.IName, &tmpItem.IData, &tmpItem.OlIID)
		if err != nil {
			return fmt.Errorf("exec %s failed %v", selectItemSQL, err)
		}
		item := itemsMap[tmpItem.OlIID]
		item.IPrice = tmpItem.IPrice
		item.IName = tmpItem.IName
		item.IData = tmpItem.IData
		item.FoundInItems = true
	}
	for i := range Items {
		item := &Items[i]
		if !item.FoundInItems {
			if item.OlIID == -1 {
				// Rollback
				return nil
			}
			return fmt.Errorf("item %d not found", item.OlIID)
		}
	}

	// Process 7
	selectStockSQL := newOrderSelectStockSQLs[len(Items)]
	selectStockArgs := make([]interface{}, len(Items)*2)
	for i := range Items {
		selectStockArgs[i*2] = Data.WID
		selectStockArgs[i*2+1] = Items[i].OlIID
	}
	rows, err = s.newOrderStmts[selectStockSQL].QueryContext(ctx, selectStockArgs...)
	if err != nil {
		return fmt.Errorf("exec %s failed %v", selectStockSQL, err)
	}
	for rows.Next() {
		var iID int
		var quantity int
		var data string
		var dists [10]string
		err = rows.Scan(&iID, &quantity, &data, &dists[0], &dists[1], &dists[2], &dists[3], &dists[4], &dists[5], &dists[6], &dists[7], &dists[8], &dists[9])
		if err != nil {
			return fmt.Errorf("exec %s failed %v", selectStockSQL, err)
		}
		item := itemsMap[iID]
		quantity -= item.OlQuantity
		if quantity < 10 {
			quantity += 91
		}
		item.FoundInStock = true
		item.SQuantity = quantity
		item.SDist = dists[Data.DID-1]
		item.OlAmount = float64(item.OlQuantity) * item.IPrice * (1 + Data.WTax + Data.DTax) * (1 - Data.CDiscount)
	}

	// Process 8
	for i := 0; i < Data.OOlCnt; i++ {
		item := &Items[i]
		if !item.FoundInStock {
			return fmt.Errorf("item (%d, %d) not found in stock", Data.WID, item.OlIID)
		}
		if item.OlIID < 0 {
			return nil
		}
		if _, err = s.newOrderStmts[newOrderUpdateStock].ExecContext(ctx, item.SQuantity, item.OlQuantity, item.RemoteWarehouse, item.OlIID, Data.WID); err != nil {
			return fmt.Errorf("exec %s failed %v", newOrderUpdateStock, err)
		}
	}

	// Process 9
	insertOrderLineSQL := newOrderInsertOrderLineSQLs[len(Items)]
	insertOrderLineArgs := make([]interface{}, len(Items)*9)
	for i := range Items {
		item := &Items[i]
		insertOrderLineArgs[i*9] = OID
		insertOrderLineArgs[i*9+1] = Data.DID
		insertOrderLineArgs[i*9+2] = Data.WID
		insertOrderLineArgs[i*9+3] = item.OlNumber
		insertOrderLineArgs[i*9+4] = item.OlIID
		insertOrderLineArgs[i*9+5] = item.OlSupplyWID
		insertOrderLineArgs[i*9+6] = item.OlQuantity
		insertOrderLineArgs[i*9+7] = item.OlAmount
		insertOrderLineArgs[i*9+8] = item.SDist
	}
	if _, err = s.newOrderStmts[insertOrderLineSQL].ExecContext(ctx, insertOrderLineArgs...); err != nil {
		return fmt.Errorf("exec %s failed %v", insertOrderLineSQL, err)
	}
	return tx.Commit()
}

func (w *Workloader) prepareNewOrder(ctx context.Context, transactionsList []interface{}) []interface{} {
	s := getTPCCState(ctx)
	// refer 2.4.1
	d := newOrderData{
		WID:    randInt(s.R, 1, w.cfg.Warehouses),
		DID:    randInt(s.R, 1, districtPerWarehouse),
		CID:    randCustomerID(s.R),
		OOlCnt: randInt(s.R, 5, 15),
	}

	rbk := randInt(s.R, 1, 100)
	allLocal := 1

	items := make([]orderItem, d.OOlCnt)

	itemsMap := make(map[int]*orderItem, d.OOlCnt)

	for i := 0; i < len(items); i++ {
		item := &items[i]
		item.OlNumber = i + 1
		if i == len(items)-1 && rbk == 1 {
			item.OlIID = -1
		} else {
			for {
				id := randItemID(s.R)
				// Find a unique ID
				if _, ok := itemsMap[id]; ok {
					continue
				}
				itemsMap[id] = item
				item.OlIID = id
				break
			}
		}

		if w.cfg.Warehouses == 1 || randInt(s.R, 1, 100) != 1 {
			item.OlSupplyWID = d.WID
		} else {
			item.OlSupplyWID = w.otherWarehouse(ctx, d.WID)
			item.RemoteWarehouse = 1
			allLocal = 0
		}

		item.OlQuantity = randInt(s.R, 1, 10)
	}

	txnParams := newOrderParameters{
		Data:     d,
		AllLocal: allLocal,
		Items:    items,
		ItemsMap: itemsMap,
	}

	transaction := NewOrderTxn{
		Params: txnParams,
		Name:   "new_order",
	}

	transactionsList = append(transactionsList, transaction)

	return transactionsList

}
