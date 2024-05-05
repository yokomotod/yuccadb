package bigquery

import (
	"context"
	"fmt"
	"time"

	"github.com/yokomotod/yuccadb"
)

type TableMapping struct {
	BQFullTableID string
	DBTableName   string
}

func (h *BQHelper) ImportTables(ctx context.Context, db *yuccadb.YuccaDB, tableMappings []TableMapping) error {
	if len(tableMappings) == 0 {
		return fmt.Errorf("no table mappings")
	}

	for _, table := range tableMappings {
		projectID, datasetID, tableID, err := h.splitFullTableID(table.BQFullTableID)
		if err != nil {
			return fmt.Errorf("splitTableID: %w", err)
		}

		tableMeta, err := h.BQClient.DatasetInProject(projectID, datasetID).Table(tableID).Metadata(ctx)
		if err != nil {
			return fmt.Errorf("Table.Metadata: %w", err)
		}

		lastSynced, ok := db.TableTimestamp(table.DBTableName)

		if ok && !tableMeta.LastModifiedTime.After(lastSynced) {
			h.Logger.Tracef("Table %q - `%s.%s.%s` is up to date, skip syncing (last modified: %s, last synced: %s)\n", table.DBTableName, projectID, datasetID, tableID, tableMeta.LastModifiedTime.Format(time.RFC3339), lastSynced.Format(time.RFC3339))
			continue
		}

		if ok {
			h.Logger.Debugf("Table %q - `%s.%s.%s` is outdated, start syncing (last modified: %s, last synced: %s)\n", table.DBTableName, projectID, datasetID, tableID, tableMeta.LastModifiedTime.Format(time.RFC3339), lastSynced.Format(time.RFC3339))
		} else {
			h.Logger.Debugf("Table %q - `%s.%s.%s` is not imported yet, start importing (last modified: %s)\n", table.DBTableName, projectID, datasetID, tableID, tableMeta.LastModifiedTime.Format(time.RFC3339))
		}

		// table name _ timestamp .csv
		filename := table.DBTableName + "_" + tableMeta.LastModifiedTime.Format("20060102150405") + ".csv"
		err = h.DownloadTableCSV(ctx, table.BQFullTableID, filename)
		if err != nil {
			return fmt.Errorf("DownloadTableCSV: %w", err)
		}

		db.PutTable(table.DBTableName, h.DownloadDir+"/"+filename, true)

		h.Logger.Infof("Imported table `%s.%s.%s` to %q\n", projectID, datasetID, tableID, table.DBTableName)
	}

	return nil
}

func (h *BQHelper) StartSyncTables(ctx context.Context, db *yuccadb.YuccaDB, tableMappings []TableMapping) (chan error, error) {
	err := h.ImportTables(ctx, db, tableMappings)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)

	go (func() {
		for {
			time.Sleep(1 * time.Minute)

			err := h.ImportTables(ctx, db, tableMappings)
			if err != nil {
				ch <- err
			}
		}
	})()

	return ch, nil
}
