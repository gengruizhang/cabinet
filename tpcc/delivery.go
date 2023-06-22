package tpcc

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type deliveryData struct {
	WID         int
	OCarrierID  int
	OlDeliveryD string
}

type deliveryParameters struct {
	Data deliveryData
}

type DeliveryTxn struct {
	Params deliveryParameters
	Name   string
}

const (
	deliverySelectNewOrder = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE"
	deliveryDeleteNewOrder = `DELETE FROM new_order WHERE (no_w_id, no_d_id, no_o_id) IN (
	(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)
)`
	deliveryUpdateOrder = `UPDATE orders SET o_carrier_id = ? WHERE (o_w_id, o_d_id, o_id) IN (
	(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)
)`
	deliverySelectOrders = `SELECT o_d_id, o_c_id FROM orders WHERE (o_w_id, o_d_id, o_id) IN (
	(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)
)`
	deliveryUpdateOrderLine = `UPDATE order_line SET ol_delivery_d = ? WHERE (ol_w_id, ol_d_id, ol_o_id) IN (
	(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)
)`
	deliverySelectSumAmount = `SELECT ol_d_id, SUM(ol_amount) FROM order_line WHERE (ol_w_id, ol_d_id, ol_o_id) IN (
	(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)
) GROUP BY ol_d_id`
	deliveryUpdateCustomer = `UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
)

func (w *Workloader) runDelivery(ctx context.Context, thread int, params deliveryParameters) error {
	s := getTPCCState(ctx)

	Data := params.Data

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	type deliveryOrder struct {
		OID    int
		CID    int
		amount float64
	}
	orders := make([]deliveryOrder, 10)
	for i := 0; i < districtPerWarehouse; i++ {
		if err = s.deliveryStmts[deliverySelectNewOrder].QueryRowContext(ctx, Data.WID, i+1).Scan(&orders[i].OID); err == sql.ErrNoRows {
			continue
		} else if err != nil {
			return fmt.Errorf("exec %s failed %v", deliverySelectNewOrder, err)
		}
	}

	if _, err = s.deliveryStmts[deliveryDeleteNewOrder].ExecContext(ctx,
		Data.WID, 1, orders[0].OID,
		Data.WID, 2, orders[1].OID,
		Data.WID, 3, orders[2].OID,
		Data.WID, 4, orders[3].OID,
		Data.WID, 5, orders[4].OID,
		Data.WID, 6, orders[5].OID,
		Data.WID, 7, orders[6].OID,
		Data.WID, 8, orders[7].OID,
		Data.WID, 9, orders[8].OID,
		Data.WID, 10, orders[9].OID,
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliveryDeleteNewOrder, err)
	}

	if _, err = s.deliveryStmts[deliveryUpdateOrder].ExecContext(ctx, Data.OCarrierID,
		Data.WID, 1, orders[0].OID,
		Data.WID, 2, orders[1].OID,
		Data.WID, 3, orders[2].OID,
		Data.WID, 4, orders[3].OID,
		Data.WID, 5, orders[4].OID,
		Data.WID, 6, orders[5].OID,
		Data.WID, 7, orders[6].OID,
		Data.WID, 8, orders[7].OID,
		Data.WID, 9, orders[8].OID,
		Data.WID, 10, orders[9].OID,
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliveryUpdateOrder, err)
	}

	if rows, err := s.deliveryStmts[deliverySelectOrders].QueryContext(ctx,
		Data.WID, 1, orders[0].OID,
		Data.WID, 2, orders[1].OID,
		Data.WID, 3, orders[2].OID,
		Data.WID, 4, orders[3].OID,
		Data.WID, 5, orders[4].OID,
		Data.WID, 6, orders[5].OID,
		Data.WID, 7, orders[6].OID,
		Data.WID, 8, orders[7].OID,
		Data.WID, 9, orders[8].OID,
		Data.WID, 10, orders[9].OID,
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliverySelectOrders, err)
	} else {
		for rows.Next() {
			var DID, CID int
			if err = rows.Scan(&DID, &CID); err != nil {
				return fmt.Errorf("exec %s failed %v", deliverySelectOrders, err)
			}
			orders[DID-1].CID = CID
		}
	}

	if _, err = s.deliveryStmts[deliveryUpdateOrderLine].ExecContext(ctx, time.Now().Format(timeFormat),
		Data.WID, 1, orders[0].OID,
		Data.WID, 2, orders[1].OID,
		Data.WID, 3, orders[2].OID,
		Data.WID, 4, orders[3].OID,
		Data.WID, 5, orders[4].OID,
		Data.WID, 6, orders[5].OID,
		Data.WID, 7, orders[6].OID,
		Data.WID, 8, orders[7].OID,
		Data.WID, 9, orders[8].OID,
		Data.WID, 10, orders[9].OID,
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliveryUpdateOrderLine, err)
	}

	if rows, err := s.deliveryStmts[deliverySelectSumAmount].QueryContext(ctx,
		Data.WID, 1, orders[0].OID,
		Data.WID, 2, orders[1].OID,
		Data.WID, 3, orders[2].OID,
		Data.WID, 4, orders[3].OID,
		Data.WID, 5, orders[4].OID,
		Data.WID, 6, orders[5].OID,
		Data.WID, 7, orders[6].OID,
		Data.WID, 8, orders[7].OID,
		Data.WID, 9, orders[8].OID,
		Data.WID, 10, orders[9].OID,
	); err != nil {
		return fmt.Errorf("exec %s failed %v", deliverySelectSumAmount, err)
	} else {
		for rows.Next() {
			var DID int
			var amount float64
			if err = rows.Scan(&DID, &amount); err != nil {
				return fmt.Errorf("exec %s failed %v", deliverySelectOrders, err)
			}
			orders[DID-1].amount = amount
		}
	}

	for i := 0; i < districtPerWarehouse; i++ {
		order := &orders[i]
		if order.OID == 0 {
			continue
		}
		if _, err = s.deliveryStmts[deliveryUpdateCustomer].ExecContext(ctx, order.amount, Data.WID, i+1, order.CID); err != nil {
			return fmt.Errorf("exec %s failed %v", deliveryUpdateCustomer, err)
		}
	}
	return tx.Commit()
}

func (w *Workloader) prepareDelivery(ctx context.Context, transactionsList []interface{}) []interface{} {
	s := getTPCCState(ctx)

	d := deliveryData{
		WID:        randInt(s.R, 1, w.cfg.Warehouses),
		OCarrierID: randInt(s.R, 1, 10),
	}

	txnParams := deliveryParameters{
		Data: d,
	}

	transaction := DeliveryTxn{
		Params: txnParams,
		Name:   "delivery",
	}

	transactionsList = append(transactionsList, transaction)

	return transactionsList

}
