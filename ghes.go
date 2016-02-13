package main

import (
	"errors"
	"flag"
	"github.com/Sirupsen/logrus"
	"github.com/pcarrier/go-github/github"
	"golang.org/x/oauth2"
	es "gopkg.in/olivere/elastic.v3"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ghToken       = flag.String("gh-token", "", "GitHub access token (from https://github.com/settings/tokens)")
	ghOrg         = flag.String("gh-org", "meteor", "GitHub organization to index")
	esURL         = flag.String("es-url", "http://localhost:9200", "ElasticSearch URLs, comma-separated")
	esIndex       = flag.String("es-index", "github", "Name of the ElasticSearch index to update")
	esUser        = flag.String("es-user", "", "ElasticSearch username")
	esPass        = flag.String("es-pass", "", "ElasticSearch password")
	esDebug       = flag.Bool("es-debug", false, "Trace all the things in ElasticSearch")
	since         = flag.Int64("since", 0, "We only look at tickets updated after that Unix timestamp")
	batchInterval = flag.Duration("batch-len", 1*time.Second, "How often we send batches, at most")
	priorityRE, _ = regexp.Compile("^P([0-9])$")
	splitRE, _    = regexp.Compile("^([^:]+):(.*)$")
)

const mapping = `
{
	"mappings": {
		"_default_": {
			"dynamic_templates": [
				{
					"strings": {
						"match_mapping_type": "string",
						"mapping": {
							"index": "not_analyzed"
						}
					}
				}
			],
			"properties": {
				"body": {"type": "string", "index": "analyzed"}
			}
		}
	}
}
`

type logger struct{}

func (l logger) Printf(format string, v ...interface{}) {
	logrus.Printf(format, v...)
}

type esIssue struct {
	Repo      string            `json:"repo"`
	Number    int               `json:"number"`
	Title     string            `json:"title"`
	State     string            `json:"state"`
	Comments  int               `json:"comments"`
	Body      *string           `json:"body"`
	CreatedAt *time.Time        `json:"created_at"`
	UpdatedAt *time.Time        `json:"updated_at,omitempty"`
	ClosedAt  *time.Time        `json:"closed_at,omitempty"`
	Reporter  string            `json:"reporter"`
	Assignee  *string           `json:"assignee,omitempty"`
	Milestone *string           `json:"milestone,omitempty"`
	Priority  *int8             `json:"priority,omitempty"`
	Labels    []string          `json:"labels"`
	Tags      map[string]string `json:"t"`
}

func githubClient() *github.Client {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: *ghToken})
	tc := oauth2.NewClient(oauth2.NoContext, ts)
	return github.NewClient(tc)
}

func scanGithub(client *github.Client, out chan<- *github.Issue) (time.Time, error) {
	options := github.IssueListOptions{
		Filter:    "all",
		State:     "all",
		Sort:      "created",
		Direction: "asc",
		Since:     time.Unix(*since, 0),
	}

	freshest := time.Time{}

	for {
		issues, resp, err := client.Issues.ListByOrg(*ghOrg, &options)
		if err != nil {
			return freshest, err
		}

		for _, issue := range issues {
			lissue := issue
			out <- &lissue
			if issue.UpdatedAt.After(freshest) {
				freshest = *issue.UpdatedAt
			}
		}

		if resp.NextPage == 0 {
			return freshest, nil
		}

		if resp.Rate.Remaining <= 1 {
			wait := resp.Rate.Reset.Sub(time.Now())
			logrus.WithField("wait", wait).Info("got rate limited")
			time.Sleep(wait)
		}

		options.ListOptions.Page = resp.NextPage
	}
}

func esClient() (*es.Client, error) {
	urls := strings.Split(*esURL, ",")
	options := []es.ClientOptionFunc{
		es.SetURL(urls...),
		es.SetSniff(false),
	}

	if *esUser != "" {
		options = append(options, es.SetBasicAuth(*esUser, *esPass))
	}

	if *esDebug {
		l := logger{}
		options = append(options,
			es.SetErrorLog(l),
			es.SetInfoLog(l),
			es.SetTraceLog(l),
		)
	}

	client, err := es.NewClient(options...)
	if err != nil {
		return nil, err
	}
	exists, err := client.IndexExists(*esIndex).Do()
	if err != nil {
		return nil, err
	}
	if !exists {
		resp, err := client.CreateIndex(*esIndex).Body(mapping).Do()
		if err != nil {
			return nil, err
		}
		if !resp.Acknowledged {
			return nil, errors.New("index creation was not acknowledged")
		}
		logrus.Info("index created")
	}

	return client, nil
}

func esBulk(client *es.Client, in <-chan es.BulkableRequest) {
	ticker := time.NewTicker(*batchInterval)

	bulk := client.Bulk()

	for {
		select {
		case <-ticker.C:
			if bulk.NumberOfActions() != 0 {
				lr := logrus.WithField("ops", bulk.NumberOfActions())
				resp, err := bulk.Do()
				if err == nil && !resp.Errors {
					lr.Info("bulk successful")
					bulk = client.Bulk()
				} else {
					lr.WithError(err).Warn("bulk failed")
				}
			}
		case issue, ok := <-in:
			if !ok {
				return
			}
			bulk.Add(issue)
		}
	}
}

func ghToEs(in <-chan *github.Issue, out chan<- es.BulkableRequest) {
	for source := range in {
		id := *source.HTMLURL

		issue := esIssue{
			Repo:      *source.Repository.FullName,
			Number:    *source.Number,
			Title:     *source.Title,
			State:     *source.State,
			Comments:  *source.Comments,
			Body:      source.Body,
			CreatedAt: source.CreatedAt,
			UpdatedAt: source.UpdatedAt,
			ClosedAt:  source.ClosedAt,
			Reporter:  *source.User.Login,
			Labels:    []string{},
			Tags:      make(map[string]string),
		}
		for _, label := range source.Labels {
			name := *label.Name
			issue.Labels = append(issue.Labels, name)
			priorityMatch := priorityRE.FindSubmatch([]byte(name))
			if priorityMatch != nil {
				prio, _ := strconv.ParseInt(string(priorityMatch[1]), 10, 8)
				prio8 := int8(prio)
				issue.Priority = &prio8
			}
			splitMatch := splitRE.FindSubmatch([]byte(name))
			if splitMatch != nil {
				issue.Tags[string(splitMatch[1])] = string(splitMatch[2])
			}
		}
		if source.Assignee != nil {
			issue.Assignee = source.Assignee.Login
		}
		if source.Milestone != nil {
			issue.Milestone = source.Milestone.Title
		}

		out <- es.NewBulkIndexRequest().Index(*esIndex).Type("issue").Id(id).Doc(issue)
	}
}

func main() {
	flag.Parse()

	esClient, err := esClient()
	if err != nil {
		logrus.WithError(err).Fatal("could not create ES client")
	}

	done := make(chan error)

	ghClient := githubClient()
	issuesChan := make(chan *github.Issue, 1024)
	esRequests := make(chan es.BulkableRequest, 1024)

	go func() {
		_, err := scanGithub(ghClient, issuesChan)
		close(issuesChan)
		if err != nil {
			logrus.WithError(err).Error("could not read from github")
			done <- err
		} else {
			logrus.Info("done scanning")
			done <- nil
		}
	}()

	go func() {
		ghToEs(issuesChan, esRequests)
		logrus.Info("done converting")
		close(esRequests)
		done <- nil
	}()

	go func() {
		esBulk(esClient, esRequests)
		logrus.Info("done indexing")
		done <- nil
	}()

	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			os.Exit(1)
		}
	}
}
