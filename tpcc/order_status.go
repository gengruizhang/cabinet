package tpcc

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	orderStatusSelectCustomerCntByLast = `SELECT count(c_id) namecnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?`
	orderStatusSelectCustomerByLast    = `SELECT c_balance, c_first, c_middle, c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first`
	orderStatusSelectCustomerByID      = `SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
	orderStatusSelectLatestOrder       = `SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1`
	orderStatusSelectOrderLine         = `SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?`
)

type orderStatusData struct {
	WID int
	DID int

	CID      int
	CLast    string
	CBalance float64
	CFirst   string
	CMiddle  string

	OID        int
	OEntryD    string
	OCarrierID sql.NullInt64
}

type orderStatusParameters struct {
	Data orderStatusData
}

type OrderStatusTxn struct {
	Params orderStatusParameters
	Name   string
}

func (w *Workloader) runOrderStatus(ctx context.Context, thread int, params orderStatusParameters) error {
	s := getTPCCState(ctx)

	Data := params.Data

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if Data.CID == 0 {
		// by Name
		// SELECT count(c_id) INTO :namecnt FROM customer
		//	WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
		var nameCnt int
		if err := s.orderStatusStmts[orderStatusSelectCustomerCntByLast].QueryRowContext(ctx, Data.WID, Data.DID, Data.CLast).Scan(&nameCnt); err != nil {
			return fmt.Errorf("exec %s failed %v", orderStatusSelectCustomerCntByLast, err)
		}
		if nameCnt%2 == 1 {
			nameCnt++
		}

		rows, err := s.orderStatusStmts[orderStatusSelectCustomerByLast].QueryContext(ctx, Data.WID, Data.DID, Data.CLast)
		if err != nil {
			return fmt.Errorf("exec %s failed %v", orderStatusSelectCustomerByLast, err)
		}
		for i := 0; i < nameCnt/2 && rows.Next(); i++ {
			if err := rows.Scan(&Data.CBalance, &Data.CFirst, &Data.CMiddle, &Data.CID); err != nil {
				return err
			}
		}

		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	} else {
		if err := s.orderStatusStmts[orderStatusSelectCustomerByID].QueryRowContext(ctx, Data.WID, Data.DID, Data.CID).Scan(&Data.CBalance, &Data.CFirst, &Data.CMiddle, &Data.CLast); err != nil {
			return fmt.Errorf("exec %s failed %v", orderStatusSelectCustomerByID, err)
		}
	}

	// SELECT o_id, o_carrier_id, o_entry_d
	//  INTO :o_id, :o_carrier_id, :entdate FROM orders
	//  ORDER BY o_id DESC;

	// refer 2.6.2.2 - select the latest order
	if err := s.orderStatusStmts[orderStatusSelectLatestOrder].QueryRowContext(ctx, Data.WID, Data.DID, Data.CID).Scan(&Data.OID, &Data.OCarrierID, &Data.OEntryD); err != nil {
		return fmt.Errorf("exec %s failed %v", orderStatusSelectLatestOrder, err)
	}

	// SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// 	ol_amount, ol_delivery_d
	// 	FROM order_line
	// 	WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// OPEN c_line;
	rows, err := s.orderStatusStmts[orderStatusSelectOrderLine].QueryContext(ctx, Data.WID, Data.DID, Data.OID)
	if err != nil {
		return fmt.Errorf("exec %s failed %v", orderStatusSelectOrderLine, err)
	}
	defer rows.Close()

	Items := make([]orderItem, 0, 4)
	for rows.Next() {
		var item orderItem
		if err := rows.Scan(&item.OlIID, &item.OlSupplyWID, &item.OlQuantity, &item.OlAmount, &item.OlDeliveryD); err != nil {
			return err
		}
		Items = append(Items, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return tx.Commit()
}

func (w *Workloader) prepareOrderStatus(ctx context.Context, transactionsList []interface{}) []interface{} {
	s := getTPCCState(ctx)
	d := orderStatusData{
		WID: randInt(s.R, 1, w.cfg.Warehouses),
		DID: randInt(s.R, 1, districtPerWarehouse),
	}

	// refer 2.6.1.2
	if s.R.Intn(100) < 60 {
		d.CLast = randCLast(s.R, s.Buf)
	} else {
		d.CID = randCustomerID(s.R)
	}

	txnParams := orderStatusParameters{
		Data: d,
	}

	transaction := OrderStatusTxn{
		Params: txnParams,
		Name:   "order_status",
	}

	transactionsList = append(transactionsList, transaction)

	return transactionsList

}
