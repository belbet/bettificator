package main

import (
	"fmt"
	"log"
	"os"
	"time"

	retrievor "github.com/belbet/retrievor"
	"github.com/caarlos0/env"
	"github.com/urfave/cli/v2"
	rdb "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

const (
	appName        string = "El retrievor"
	appDescription string = "Retrieve, parse, and insert data from football database."
	appVersion     string = "0.1"
)

const helpTemplate = `
Usage: {{.HelpName}} [command]
{{if .Commands}}Commands:
{{range .Commands}}{{if not .HideHelp}}{{join .Names ", "}}{{ "\t"}}{{.Usage}}{{ "\n" }}{{end}}{{end}}{{end}}
`

// DateStruct Data struct for the start and end dates
type DateStruct struct {
	StartDate struct {
		Year  string `json:"year"`
		Month string `json:"month"`
		Day   string `json:"day"`
	} `json:"start_date"`
	EndDate struct {
		Year  string `json:"year"`
		Month string `json:"month"`
		Day   string `json:"day"`
	} `json:"end_date"`
}

type jobStruct struct {
	t time.Time
}

type database struct {
	Host     string `long:"host" description:"the IP to listen on" default:"localhost" env:"DB_HOST" envDefault:"localhost"`
	Port     string `long:"port" description:"the port to connect to" env:"DB_PORT" envDefault:"28015"`
	User     string `long:"user" description:"the user to connect to the db with" env:"DB_USER" envDefault:"admin"`
	Password string `long:"password" description:"the password to connect to the db" env:"DB_PASS"`
	Type     string `long:"type" description:"the type of database used: only rethinkdb for now" env:"DB_TYPE" envDefault:"rethinkdb"`
	Db       string `long:"db" description:"the name of the db" env:"DB_NAME" envDefault:"test"`
	Table    string `long:"table" description:"the name of the table" env:"DB_TABLE" envDefault:"test"`
}

func getDatabaseConfig() database {
	d := database{}
	if err := env.Parse(&d); err != nil {
		log.Fatalln(err)
	}
	return d
}

var (
	d          = getDatabaseConfig()
	session, _ = rdb.Connect(rdb.ConnectOpts{
		Address:  d.Host + ":" + d.Port, // endpoint without http
		Username: d.User,
		Password: d.Password,
	})
)

func clubs(c *cli.Context) error {
	log.Println("Start clubs retrieving...")
	var p = retrievor.ClubParse{}
	dryRun := c.String("dry-run")
	var countries = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "1"}
	// Iterate trough all pages
	for _, e := range countries {
		p.CurrentPage = e
		p.ParseAll()
	}
	if dryRun != "true" {
		err := rdb.DB(d.Db).Table(d.Table).Insert(p.Clubs).Exec(session)
		if err != nil {
			return err
		}
	}
	return nil
}

func worker(job <-chan jobStruct, results chan<- bool, c *cli.Context) {
	for j := range job {
		r := retrievor.MatchesResult{}
		date := j.t.Format("2006-01-02")
		dryRun := c.String("dry-run")
		err := r.ParsePage(date)
		if err != nil {
			log.Fatalln(err)
		}
		if dryRun != "true" {
			err := rdb.DB(d.Db).Table(d.Table).Insert(r.Matches).Exec(session)
			if err != nil {
				log.Fatalln(err)
			}
		}
		results <- true
	}
}

func matches(c *cli.Context) error {
	log.Println("Start matches retrieving...")
	startDate := c.String("start-date")
	endDate := c.String("end-date")
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return err
	}
	end, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return err
	}
	// Init threads
	const threads = 8
	nDays := int(end.Sub(start).Hours() / 24)
	jobs := make(chan jobStruct, nDays)
	results := make(chan bool, nDays)
	for i := 0; i < threads; i++ {
		go worker(jobs, results, c)
	}

	// Start parsing
	for t := start; t.After(end) == false; t = t.AddDate(0, 0, 1) {
		job := jobStruct{
			t: t,
		}
		jobs <- job
	}
	close(jobs)
	for i := 0; i < nDays; i++ {
		<-results
	}
	return nil
}

func main() {
	cli.AppHelpTemplate = fmt.Sprintf(helpTemplate)
	app := cli.NewApp()
	app.Name = appName
	app.Usage = appDescription
	app.Version = appVersion

	app.Commands = []*cli.Command{
		{
			Name:    "retrieve",
			Aliases: []string{"r"},
			Usage:   "Fetch required data. ",
			Subcommands: []*cli.Command{
				{
					Name:   "clubs",
					Usage:  "Retrieve all clubs",
					Action: clubs,
					Flags: []cli.Flag{
						&cli.BoolFlag{
							Name:        "dry-run",
							Usage:       "Parse all clubs does not insert into database",
							Aliases:     []string{"d"},
							Required:    false,
							DefaultText: "false",
						},
					},
				},
				{
					Name:   "matches",
					Usage:  "Retrieve matches between startDate and endDate",
					Action: matches,
					Flags: []cli.Flag{
						&cli.StringFlag{
							Name:        "start-date",
							Usage:       "Starting date for parsing. Format: \"2006-01-02\"",
							Aliases:     []string{"s"},
							Required:    true,
							DefaultText: "2009-01-31",
						},
						&cli.StringFlag{
							Name:        "end-date",
							Usage:       "End date for parsing. Format: \"2006-01-02\"",
							Aliases:     []string{"e"},
							Required:    true,
							DefaultText: "2020-12-31",
						},
						&cli.BoolFlag{
							Name:        "dry-run",
							Usage:       "Parse all matches in date range but does not insert into database",
							Aliases:     []string{"d"},
							Required:    false,
							DefaultText: "false",
						},
					},
				},
			},
		},
	}
	log.Println("Bettificator")
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
