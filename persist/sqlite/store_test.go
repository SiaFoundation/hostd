package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"go.sia.tech/hostd/v2/host/settings"
	"go.uber.org/zap/zaptest"
)

func TestBackup(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenDatabase(srcPath, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// add some data to the database
	id, err := db.AddVolume("foo", false)
	if err != nil {
		t.Fatal(err)
	} else if err = db.GrowVolume(id, 100); err != nil {
		t.Fatal(err)
	} else if err = db.SetAvailable(id, true); err != nil {
		t.Fatal(err)
	}

	volume, err := db.Volume(id)
	if err != nil {
		t.Fatal(err)
	}

	newSettings := settings.Settings{
		MaxContractDuration: 100,
		NetAddress:          "foo.bar.baz",
		AcceptingContracts:  true,
	}
	if err := db.UpdateSettings(newSettings); err != nil {
		t.Fatal(err)
	}

	checkDatabase := func(t *testing.T, fp string) {
		// open the backup database
		backup, err := OpenDatabase(fp, zaptest.NewLogger(t))
		if err != nil {
			t.Fatal(err)
		}
		defer backup.Close()

		// check that the data was backed up correctly
		restoredVolume, err := backup.Volume(id)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(volume, restoredVolume) {
			t.Fatalf("expected volume %v, got %v", volume, restoredVolume)
		}

		restoredSettings, err := backup.Settings()
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(newSettings, restoredSettings) {
			t.Fatalf("expected settings %v, got %v", newSettings, restoredSettings)
		}
	}

	t.Run("Store.Backup", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.db")
		if err := Backup(context.Background(), srcPath, destPath); err != nil {
			t.Fatal(err)
		}

		checkDatabase(t, destPath)
	})

	t.Run("Backup", func(t *testing.T) {
		destPath := filepath.Join(t.TempDir(), "backup.db")
		if err := db.Backup(context.Background(), destPath); err != nil {
			t.Fatal(err)
		}

		checkDatabase(t, destPath)
	})
}
