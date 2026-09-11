package database

import (
	"testing"

	"gorm.io/gorm"
)

type compositeIndexDialector struct {
	gorm.Dialector
	indexes []CompositeIndex
}

func (d *compositeIndexDialector) Name() string { return "custom" }

func (d *compositeIndexDialector) EnsureCompositeIndexes(_ *gorm.DB, indexes []CompositeIndex) error {
	d.indexes = indexes
	return nil
}

func TestEnsureCompositeIndexesUsesDialectorEnsurer(t *testing.T) {
	dialector := &compositeIndexDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}
	want := []CompositeIndex{{Table: "widgets", Name: "widgets_owner", Columns: "tenant_id, owner_id"}}

	if err := EnsureCompositeIndexes(db, want); err != nil {
		t.Fatalf("EnsureCompositeIndexes() error = %v, want nil", err)
	}
	if len(dialector.indexes) != 1 || dialector.indexes[0] != want[0] {
		t.Fatalf("EnsureCompositeIndexes() indexes = %#v, want %#v", dialector.indexes, want)
	}
}

type compositeUniqueIndexDialector struct {
	gorm.Dialector
	indexes []CompositeUniqueIndex
}

func (d *compositeUniqueIndexDialector) Name() string { return "custom" }

func (d *compositeUniqueIndexDialector) EnsureCompositeUniqueIndexes(_ *gorm.DB, indexes []CompositeUniqueIndex) error {
	d.indexes = indexes
	return nil
}

func TestEnsureCompositeUniqueIndexesUsesDialectorEnsurer(t *testing.T) {
	dialector := &compositeUniqueIndexDialector{}
	db := &gorm.DB{Config: &gorm.Config{Dialector: dialector}}
	want := []CompositeUniqueIndex{{Table: "widgets", Name: "widgets_tenant_slug", Columns: "tenant_id, slug"}}

	if err := EnsureCompositeUniqueIndexes(db, want); err != nil {
		t.Fatalf("EnsureCompositeUniqueIndexes() error = %v, want nil", err)
	}
	if len(dialector.indexes) != 1 || dialector.indexes[0] != want[0] {
		t.Fatalf("EnsureCompositeUniqueIndexes() indexes = %#v, want %#v", dialector.indexes, want)
	}
}
