package tpcc

import (
	"context"
	"fmt"
	"time"
)

const (
	paymentUpdateDistrict           = `UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?`
	paymentSelectDistrict           = `SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?`
	paymentUpdateWarehouse          = `UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?`
	paymentSelectWarehouse          = `SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?`
	paymentSelectCustomerListByLast = `SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first`
	paymentSelectCustomerForUpdate  = `SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? 
AND c_id = ? FOR UPDATE`
	paymentUpdateCustomer = `UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, 
c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
	paymentSelectCustomerData     = `SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
	paymentUpdateCustomerWithData = `UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, 
c_payment_cnt = c_payment_cnt + 1, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?`
	paymentInsertHistory = `INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

type paymentData struct {
	WID     int
	DID     int
	CWID    int
	CDID    int
	HAmount float64

	WStreet1 string
	WStreet2 string
	WCity    string
	WState   string
	WZip     string
	WName    string

	DStreet1 string
	DStreet2 string
	DCity    string
	DState   string
	DZip     string
	DName    string

	CID        int
	CFirst     string
	CMiddle    string
	CLast      string
	CStreet1   string
	CStreet2   string
	CCity      string
	CState     string
	CZip       string
	CPhone     string
	CSince     string
	CCredit    string
	CCreditLim float64
	CDiscount  float64
	CBalance   float64
	CData      string
}

type paymentParameters struct {
	Data paymentData
}

type PaymentTxn struct {
	Params paymentParameters
	Name   string
}

func (w *Workloader) runPayment(ctx context.Context, thread int, params paymentParameters) error {
	s := getTPCCState(ctx)

	Data := params.Data

	tx, err := w.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Process 1
	if _, err := s.paymentStmts[paymentUpdateDistrict].ExecContext(ctx, Data.HAmount, Data.WID, Data.DID); err != nil {
		return fmt.Errorf("exec %s failed %v", paymentUpdateDistrict, err)
	}

	// Process 2
	if err := s.paymentStmts[paymentSelectDistrict].QueryRowContext(ctx, Data.WID, Data.DID).Scan(&Data.DStreet1, &Data.DStreet2,
		&Data.DCity, &Data.DState, &Data.DZip, &Data.DName); err != nil {
		return fmt.Errorf("exec %s failed %v", paymentSelectDistrict, err)
	}

	// Process 3
	if _, err := s.paymentStmts[paymentUpdateWarehouse].ExecContext(ctx, Data.HAmount, Data.WID); err != nil {
		return fmt.Errorf("exec %s failed %v", paymentUpdateWarehouse, err)
	}

	// Process 4
	if err := s.paymentStmts[paymentSelectWarehouse].QueryRowContext(ctx, Data.WID).Scan(&Data.WStreet1, &Data.WStreet2,
		&Data.WCity, &Data.WState, &Data.WZip, &Data.WName); err != nil {
		return fmt.Errorf("exec %s failed %v", paymentSelectDistrict, err)
	}

	if Data.CID == 0 {
		// Process 5
		rows, err := s.paymentStmts[paymentSelectCustomerListByLast].QueryContext(ctx, Data.CWID, Data.CDID, Data.CLast)
		if err != nil {
			return fmt.Errorf("exec %s failed %v", paymentSelectCustomerListByLast, err)
		}
		var ids []int
		for rows.Next() {
			var id int
			if err = rows.Scan(&id); err != nil {
				return fmt.Errorf("exec %s failed %v", paymentSelectCustomerListByLast, err)
			}
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			return fmt.Errorf("customer for (%d, %d, %s) not found", Data.CWID, Data.CDID, Data.CLast)
		}
		Data.CID = ids[(len(ids)+1)/2-1]
	}

	// Process 6
	if err := s.paymentStmts[paymentSelectCustomerForUpdate].QueryRowContext(ctx, Data.CWID, Data.CDID, Data.CID).Scan(&Data.CFirst, &Data.CMiddle, &Data.CLast,
		&Data.CStreet1, &Data.CStreet2, &Data.CCity, &Data.CState, &Data.CZip, &Data.CPhone, &Data.CCredit, &Data.CCreditLim,
		&Data.CDiscount, &Data.CBalance, &Data.CSince); err != nil {
		return fmt.Errorf("exec %s failed %v", paymentSelectCustomerForUpdate, err)
	}

	if Data.CCredit == "BC" {
		// Process 7
		if err := s.paymentStmts[paymentSelectCustomerData].QueryRowContext(ctx, Data.CWID, Data.CDID, Data.CID).Scan(&Data.CData); err != nil {
			return fmt.Errorf("exec %s failed %v", paymentSelectCustomerData, err)
		}

		newData := fmt.Sprintf("| %4d %2d %4d %2d %4d $%7.2f %12s %24s", Data.CID, Data.CDID, Data.CWID,
			Data.DID, Data.WID, Data.HAmount, time.Now().Format(timeFormat), Data.CData)
		if len(newData) >= 500 {
			newData = newData[0:500]
		} else {
			newData += Data.CData[0 : 500-len(newData)]
		}

		// Process 8
		if _, err := s.paymentStmts[paymentUpdateCustomerWithData].ExecContext(ctx, Data.HAmount, Data.HAmount, newData, Data.CWID, Data.CDID, Data.CID); err != nil {
			return fmt.Errorf("exec %s failed %v", paymentUpdateCustomerWithData, err)
		}
	} else {
		// Process 9
		if _, err := s.paymentStmts[paymentUpdateCustomer].ExecContext(ctx, Data.HAmount, Data.HAmount, Data.CWID, Data.CDID, Data.CID); err != nil {
			return fmt.Errorf("exec %s failed %v", paymentUpdateCustomer, err)
		}
	}

	// Process 10
	hData := fmt.Sprintf("%10s    %10s", Data.WName, Data.DName)
	if _, err := s.paymentStmts[paymentInsertHistory].ExecContext(ctx, Data.CDID, Data.CWID, Data.CID, Data.DID, Data.WID, time.Now().Format(timeFormat), Data.HAmount, hData); err != nil {
		return fmt.Errorf("exec %s failed %v", paymentInsertHistory, err)
	}

	return tx.Commit()
}

func (w *Workloader) preparePayment(ctx context.Context, transactionsList []interface{}) []interface{} {

	s := getTPCCState(ctx)

	d := paymentData{
		WID:     randInt(s.R, 1, w.cfg.Warehouses),
		DID:     randInt(s.R, 1, districtPerWarehouse),
		HAmount: float64(randInt(s.R, 100, 500000)) / float64(100.0),
	}

	// Refer 2.5.1.2, 60% by last Name, 40% by customer ID
	if s.R.Intn(100) < 60 {
		d.CLast = randCLast(s.R, s.Buf)
	} else {
		d.CID = randCustomerID(s.R)
	}

	// Refer 2.5.1.2, 85% by local, 15% by remote
	if w.cfg.Warehouses == 1 || s.R.Intn(100) < 85 {
		d.CWID = d.WID
		d.CDID = d.DID
	} else {
		d.CWID = w.otherWarehouse(ctx, d.WID)
		d.CDID = randInt(s.R, 1, districtPerWarehouse)
	}

	txnParams := paymentParameters{
		Data: d,
	}

	transaction := PaymentTxn{
		Params: txnParams,
		Name:   "payment",
	}

	transactionsList = append(transactionsList, transaction)

	return transactionsList

}
