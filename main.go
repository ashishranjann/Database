package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

// const (
// 	version = "1.0.0"
// )

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct {
		mutex   sync.Mutex // write-delete
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}
	if options != nil {
		opts = *options
	}

	if options == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}
	driver := Driver{
		mutex:   sync.Mutex{},
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using : Databse already exists.\n", dir)
		return &driver, nil
	}
	opts.Logger.Debug("Creating the database at :\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

func (d *Driver) Write(collection, resources string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection- no place to save")
	}
	if resources == "" {
		return fmt.Errorf("missing resources- Ntg to save")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resources+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil
	}
	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if resource == "" {
		return fmt.Errorf("missing Resources- Ntg to read")
	}
	if collection == "" {
		return fmt.Errorf("collection is empty- Ntg to read")
	}
	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}
	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &v)

}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection is missing- unable to ReadAll")
	}
	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}
	files, _ := ioutil.ReadDir(dir)
	var records []string

	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		records = append(records, string(b))
	}
	return records, nil
}

func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)
	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file or directory")

	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}
	return nil

}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

// func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
// 	d.mutex.Lock()
// 	defer d.mutex.Unlock()

// 	m, ok := d.mutexes[collection]
// 	if !ok {
// 		m = &sync.Mutex{}
// 		d.mutexes[collection] = m
// 	}

// 	return m
// }

func main() {
	fmt.Println("Hello, welcome to Database:")
	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error", err)
	}

	employee := []User{
		{"John", "56", "2873683", "nioi", Address{"a", "hjk", "bsjak", "789"}},
		{"Alice", "30", "1234567", "xyz", Address{"b", "pqr", "country2", "456"}},
		{"Bob", "42", "9876543", "abc", Address{"c", "lmn", "country3", "123"}},
		{"Charlie", "25", "5555555", "def", Address{"d", "opq", "country4", "789"}},
		{"Diana", "35", "9999999", "ghi", Address{"e", "rst", "country5", "321"}},
		{"Eve", "50", "1111111", "jkl", Address{"f", "uvw", "country6", "654"}},
	}

	for _, v := range employee {
		// fmt.Println(v)
		db.Write("Users", v.Name, User{
			Name:    v.Name,
			Age:     v.Age,
			Contact: v.Contact,
			Company: v.Company,
			Address: v.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error", err)
	}
	fmt.Println(records)

	allusers := []User{}

	for _, f := range records {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println("Error", err)
		}
		allusers = append(allusers, employeeFound)
	}
	fmt.Println("Printing all users:\n", allusers)

	if err := db.Delete("users", "John"); err != nil {
		fmt.Println("Error", err)
	}
	if err := db.Delete("users", ""); err != nil {
		fmt.Println("Error", err)
	}

}
