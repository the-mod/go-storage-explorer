package main

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"

	pipeline "github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	az "github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
)

type blob struct {
	Name       string            `json:"name"`
	Content    []byte            `json:"content"`
	Properties map[string]string `json:"Properties"`
	Metadata   map[string]string `json:"metadata"`
}

type container struct {
	Name  string `json:"name"`
	Blobs []blob `json:"blobs"`
}

type storageAccount struct {
	Name      string      `json:"name"`
	Container []container `json:"container"`
}

type filter struct {
	Key   string
	Value string
}

type arguments struct {
	AccountName    string
	AccessKey      string
	ContainerName  string
	BlobName       string
	ShowContent    bool
	MetadataFilter []string
}

var largs = arguments{}

var rootCmd = &cobra.Command{
	Use:   "go-storage-explorer",
	Short: "go-storage-explorer shows containers and blobs of a azure storage account",
	Long: `go-storage-explorer shows containers and blobs of a azure storage account.
Complete documentation is available at http://hugo.spf13.com`,
	Run: func(cmd *cobra.Command, args []string) {
		exec(largs)
	},
}

const (
	storageURLTemplate   = "https://%s.blob.core.windows.net"
	containerURLTemplate = "https://%s.blob.core.windows.net/%s"
)

func init() {
	rootCmd.Flags().StringVarP(&largs.AccountName, "accountName", "n", "", "accountName of the Storage Account")
	rootCmd.Flags().StringVarP(&largs.AccessKey, "accessKey", "k", "", "accessKey for the Storage Account")
	rootCmd.Flags().StringVarP(&largs.ContainerName, "container", "c", "", "filter for container name with substring match")
	rootCmd.Flags().StringVarP(&largs.BlobName, "blob", "b", "", "filter for blob name with substring match")
	rootCmd.Flags().BoolVar(&largs.ShowContent, "show-content", false, "downloads and prints content of blobs in addition to other logs")
	rootCmd.Flags().StringSliceVarP(&largs.MetadataFilter, "metadata-filter", "m", []string{}, "OR filter for blob metadata. Structure is <key>:<value>")
	rootCmd.MarkFlagRequired("accountName")
	rootCmd.MarkFlagRequired("accessKey")
}

func downloadBlob(blobName string, containerUrl az.ContainerURL) []byte {
	blobURL := containerUrl.NewBlockBlobURL(blobName)
	downloadResponse, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)

	if err != nil {
		log.Fatalf("Error downloading blob %s", blobName)
	}

	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	downloadedData := bytes.Buffer{}
	_, err = downloadedData.ReadFrom(bodyStream)

	if err != nil {
		log.Fatalf("Error reading blob %s", blobName)
	}

	return downloadedData.Bytes()
}

func parseContainer(azContainer az.ContainerItem, p pipeline.Pipeline, accountName string, containerFilter string, blobFilter string, showContent bool, c chan *container, wg *sync.WaitGroup, marker az.Marker, metadataFilter []filter) {
	defer wg.Done()
	containerName := azContainer.Name

	// TODO substring match? to match containers: ['test-1', 'test-2'], term: 'test, matches ['test-1', 'test-2']
	if len(containerFilter) > 0 && !strings.Contains(containerName, containerFilter) {
		return
	}

	// new returns pointer to the container instance
	containerResult := new(container)
	containerResult.Name = containerName

	containerURL, _ := url.Parse(fmt.Sprintf(containerURLTemplate, accountName, containerName))
	containerServiceURL := azblob.NewContainerURL(*containerURL, p)

	ctx := context.Background()

	for blobMarker := (azblob.Marker{}); blobMarker.NotDone(); {
		listBlob, _ := containerServiceURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}})
		blobMarker = listBlob.NextMarker
		blobItems := listBlob.Segment.BlobItems
		containerResult.Blobs = parseBlobs(blobItems, blobFilter, showContent, containerServiceURL, metadataFilter)
	}

	c <- containerResult
}

func parseBlobs(blobItems []az.BlobItemInternal, blobFilter string, showContent bool, containerURL azblob.ContainerURL, metadataFilter []filter) []blob {
	var blobWg sync.WaitGroup
	bc := make(chan *blob)

	var blobs []blob

	for _, blobItem := range blobItems {
		if len(blobFilter) > 0 && !strings.Contains(blobItem.Name, blobFilter) {
			continue
		}
		blobWg.Add(1)
		go createBlobOutput(blobItem, &blobWg, bc, showContent, containerURL, metadataFilter)
	}

	go func() {
		blobWg.Wait()
		close(bc)
	}()

	for elem := range bc {
		blobs = append(blobs, *elem)
	}
	return blobs
}

func parseBlobProperties(properties az.BlobProperties) map[string]string {
	result := make(map[string]string)

	result["Blob Type"] = string(properties.BlobType)
	result["Content MD5"] = b64.StdEncoding.EncodeToString(properties.ContentMD5)
	result["Created at"] = properties.CreationTime.String()
	result["Last modified at"] = properties.LastModified.String()
	result["Lease Status"] = string(properties.LeaseStatus)
	result["Lease State"] = string(properties.LeaseState)
	result["Lease Duration"] = string(properties.LeaseDuration)

	return result
}

func containsMetadataMatch(metadata map[string]string, filter []filter) bool {
	if len(metadata) == 0 {
		return false
	}

	for _, entry := range filter {
		if val, ok := metadata[entry.Key]; ok {
			if strings.Contains(val, entry.Value) {
				return true
			}
		}
	}
	return false
}

func createBlobOutput(blobItem az.BlobItemInternal, wg *sync.WaitGroup, c chan *blob, downloadContent bool, containerURL azblob.ContainerURL, metadataFilter []filter) {
	defer wg.Done()

	if len(metadataFilter) == 0 || (len(metadataFilter) > 0 && containsMetadataMatch(blobItem.Metadata, metadataFilter)) {
		blob := new(blob)
		blob.Name = blobItem.Name
		blob.Properties = parseBlobProperties(blobItem.Properties)
		blob.Metadata = blobItem.Metadata

		if downloadContent {
			blob.Content = downloadBlob(blobItem.Name, containerURL)
		}

		c <- blob
	}
}

func createMetadataFilter(inputFilter []string) []filter {
	var metadataFilter []filter
	if len(inputFilter) > 0 {
		for _, entry := range largs.MetadataFilter {
			if strings.Contains(entry, ":") {
				split := strings.Split(entry, ":")
				f := filter{split[0], split[1]}
				metadataFilter = append(metadataFilter, f)
			}
		}
	}
	return metadataFilter
}

func exec(args arguments) {
	ctx := context.Background()

	// Create a default request pipeline using your storage account name and account key
	credential, authErr := azblob.NewSharedKeyCredential(args.AccountName, args.AccessKey)
	if authErr != nil {
		log.Fatal("Error while Authentication")
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint
	URL, _ := url.Parse(fmt.Sprintf(storageURLTemplate, args.AccountName))

	serviceURL := azblob.NewServiceURL(*URL, p)

	s := new(storageAccount)
	s.Name = URL.String()
	var foundContainer []container

	metadataFilter := createMetadataFilter(args.MetadataFilter)

	c := make(chan *container)
	var wg sync.WaitGroup
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listContainer, err := serviceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})

		if err != nil {
			log.Fatal("Error while getting Container")
		}

		for _, val := range listContainer.ContainerItems {
			wg.Add(1)
			go parseContainer(val, p, args.AccountName, args.ContainerName, args.BlobName, args.ShowContent, c, &wg, marker, metadataFilter)
		}
		// used for Pagination
		marker = listContainer.NextMarker
	}

	// wait for all entries in waitgroup and close then the channel
	go func() {
		wg.Wait()
		close(c)
	}()

	// channel to collect results
	for elem := range c {
		foundContainer = append(foundContainer, *elem)
	}

	s.Container = foundContainer
	print(*s)
}

func print(sa storageAccount) {
	m, _ := json.Marshal(sa)
	fmt.Println(string(m))
}

// kudos to:
// https://github.com/Azure/azure-storage-blob-go/blob/456ab4777f89ceb54316ddf71d2acfd39bb86e1d/azblob/zt_examples_test.go
// and
// https://github.com/Azure-Samples/storage-blobs-go-quickstart/blob/master/storage-quickstart.go
func main() {
	rootCmd.Execute()
}
