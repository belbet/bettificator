package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

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

func (ds *DateStruct) setDates(s string, e string) {
	sd := strings.Split(s, "-")
	ds.StartDate.Year = sd[0]
	ds.StartDate.Month = sd[1]
	ds.StartDate.Day = sd[2]

	ed := strings.Split(e, "-")
	ds.EndDate.Year = ed[0]
	ds.EndDate.Month = ed[1]
	ds.EndDate.Day = ed[2]
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

// Handler is executed by AWS Lambda in the main function. Once the request
// is processed, it returns an Amazon API Gateway response object to AWS Lambda
func retrieve(c *cli.Context) error {
	fmt.Println("Bettificator")
	var b = &DateStruct{}
	startDate := c.String("start-date")
	endDate := c.String("end-date")
	dryRun := c.String("dry-run")
	b.setDates(startDate, endDate)
	endDay, _ := strconv.Atoi(b.EndDate.Day)
	endMonth, _ := strconv.Atoi(b.EndDate.Month)
	endYear, _ := strconv.Atoi(b.EndDate.Year)
	br := false
	y, _ := strconv.Atoi(b.StartDate.Year)
	m, _ := strconv.Atoi(b.StartDate.Month)
	dd, _ := strconv.Atoi(b.StartDate.Day)
	for ; y <= endYear; y++ {
		for ; m <= 12; m++ {
			for ; dd <= 31; dd++ {
				if dd == endDay && m == endMonth && y == endYear {
					br = true
					break
				}
				r := retrievor.MatchesResult{}
				date := fmt.Sprintf("%d-%02d-%02d", y, m, dd)
				// := strconv.Itoa(y) + "-" + strconv.Itoa(m) + "-" + strconv.Itoa(dd)
				r.ParsePage(date)
				if dryRun != "true" {
					err := rdb.DB(d.Db).Table(d.Table).Insert(r.Matches).Exec(session)
					if err != nil {
						return err
					}
				}
			}
			dd = 1
			if br {
				break
			}
		}
		m = 1
		if br {
			break
		}
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
			Name:   "retrieve",
			Usage:  "Retrieve matches between startDate and endDate",
			Action: retrieve,
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
	}
	log.Println("Start retrieving...")
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
