package elastic

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

type IndexName string

func Index[T any](c *elastic.Client, id string, index IndexName, obj T) error {
	_, err := c.Index().
		Index(string(index)).
		Id(id).
		BodyJson(obj).
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "while indexing %s:%s", index, id)
	}

	return nil
}

func Update[T any](c *elastic.Client, id string, indexName IndexName, obj T) error {
	_, err := c.Update().
		Index(string(indexName)).
		Id(id).
		Doc(obj).
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "while updating %s:%s", indexName, id)
	}

	return nil
}

func ListAll[T any](c *elastic.Client, indexName IndexName) ([]T, error) {
	result := make([]T, 0)

	resp, err := c.Search(string(indexName)).
		Query(elastic.NewMatchAllQuery()).Do(context.Background())
	if err != nil {
		return nil, errors.Wrapf(err, "while listing %s", indexName)
	}
	for _, hit := range resp.Hits.Hits {
		var idx T
		err := json.Unmarshal(hit.Source, &idx)
		if err != nil {
			return nil, errors.Wrap(err, "while decoding search result")
		}
		result = append(result, idx)
	}

	return result, nil
}

func Delete(c *elastic.Client, id string, indexName IndexName) error {
	_, err := c.Delete().
		Index(string(indexName)).
		Id(id).
		Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "while deleting index %s:%s", indexName, id)
	}

	return nil
}

func Drop(c *elastic.Client, indexNames ...IndexName) error {
	idxToDelete := make([]string, 0)
	for _, name := range indexNames {
		idxToDelete = append(idxToDelete, string(name))
	}

	_, err := c.DeleteIndex(idxToDelete...).Do(context.Background())
	if err != nil {
		return errors.Wrapf(err, "while droping indexes: %v", idxToDelete)
	}

	return nil
}
