package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/discovery"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/pg"
)

// discoveryKeywords runs `fbctl discovery keywords seed|import|list`.
//
//	seed                 load the Iraq keywords (configs/discovery/iraq.yaml)
//	import --file F      load a seed file (YAML, same format) or a CSV of
//	                     term,language[,group[,enabled]]
//	list [--language L] [--group G]
//
// Loading adds only keywords not listed yet; listed ones keep their state.
func discoveryKeywords(ctx context.Context, cfg *config.Config, args []string) error {
	if len(args) == 0 {
		return errors.New("discovery keywords: want seed, import --file F, or list")
	}
	action, rest := args[0], args[1:]
	fs := flag.NewFlagSet("discovery keywords "+action, flag.ContinueOnError)
	file := fs.String("file", "", "seed YAML or CSV (term,language,group,enabled)")
	lang := fs.String("language", "", "list: only this language (en, ar, ckb, kmr)")
	group := fs.String("group", "", "list: only this group")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	db, err := pg.Open(ctx, cfg.Postgres.DSN, 4)
	if err != nil {
		return err
	}
	defer db.Close()
	var seed []discovery.SeedKeyword
	switch action {
	case "seed":
		seed, err = discovery.IraqSeed()
	case "import":
		if *file == "" {
			return errors.New("discovery keywords import: --file is required")
		}
		seed, err = readKeywordFile(*file)
	case "list":
		kws, err := discovery.ListKeywords(ctx, db, discovery.KeywordFilter{Language: *lang, Group: *group, Limit: 100000})
		if err != nil {
			return err
		}
		printJSON(map[string]any{"keywords": kws})
		return nil
	default:
		return fmt.Errorf("discovery keywords: unknown action %q (seed, import, list)", action)
	}
	if err != nil {
		return err
	}
	added, err := discovery.ImportSeed(ctx, db, seed)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"keywords": len(seed), "added": added, "already_listed": len(seed) - added})
	return nil
}

func readKeywordFile(path string) ([]discovery.SeedKeyword, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if !strings.HasSuffix(strings.ToLower(path), ".csv") {
		b, err := io.ReadAll(f)
		if err != nil {
			return nil, err
		}
		return discovery.ParseSeed(b)
	}
	return parseKeywordCSV(f)
}

// parseKeywordCSV reads term,language[,group[,enabled]] rows; a first row
// whose second column isn't a language is a header.
func parseKeywordCSV(r io.Reader) ([]discovery.SeedKeyword, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	rows, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	var out []discovery.SeedKeyword
	for i, row := range rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("line %d: want term,language[,group[,enabled]]", i+1)
		}
		lang := strings.TrimSpace(row[1])
		if !discovery.Languages[lang] {
			if i == 0 {
				continue // header
			}
			return nil, fmt.Errorf("line %d: language %q must be en, ar, ckb or kmr", i+1, lang)
		}
		k := discovery.SeedKeyword{Term: strings.TrimSpace(row[0]), Language: lang, Enabled: true}
		if len(row) > 2 {
			k.Group = strings.TrimSpace(row[2])
		}
		if len(row) > 3 && strings.TrimSpace(row[3]) != "" {
			if k.Enabled, err = strconv.ParseBool(strings.TrimSpace(row[3])); err != nil {
				return nil, fmt.Errorf("line %d: enabled %q: %w", i+1, row[3], err)
			}
		}
		out = append(out, k)
	}
	if len(out) == 0 {
		return nil, errors.New("no keywords in the file")
	}
	return out, nil
}
