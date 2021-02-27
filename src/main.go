package main

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	pipeline "github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	az "github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
)

type blob struct {
	Name       string            `json:"name"`
	Content    bytes.Buffer      `json:"content"`
	Properties map[string]string `json:"Properties"`
}

type container struct {
	Name  string `json:"name"`
	Blobs []blob `json:"blobs"`
}

type storageAccount struct {
	Container []container `json:"container"`
}

type arguments struct {
	AccountName   string
	AccessKey     string
	ContainerName string
	BlobName      string
	ShowContent   bool
	StoreContent  bool
	ContentOnly   bool
	FileName      string
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

var defaultFileName = "blobcontent.txt"

func init() {
	rootCmd.Flags().StringVarP(&largs.AccountName, "accountName", "n", "", "accountName of the Storage Account")
	rootCmd.Flags().StringVarP(&largs.AccessKey, "accessKey", "k", "", "accessKey for the Storage Account")
	rootCmd.Flags().StringVarP(&largs.ContainerName, "container", "c", "", "filter for container name with substring match")
	rootCmd.Flags().StringVarP(&largs.BlobName, "blob", "b", "", "filter for blob name with substring match")
	rootCmd.Flags().BoolVar(&largs.ShowContent, "show-content", false, "downloads and prints content of blobs in addition to other logs")
	rootCmd.Flags().BoolVar(&largs.StoreContent, "store-content", false, "downloads and stores content of blob in a file. Use --filename or -f to set a specific filename. Stores one line for each blob")
	rootCmd.Flags().StringVarP(&largs.FileName, "filename", "f", "", "in addtion")
	rootCmd.Flags().BoolVar(&largs.ContentOnly, "content-only", false, "prints only content of blob. overrules --show-content")
	rootCmd.MarkFlagRequired("accountName")
	rootCmd.MarkFlagRequired("accessKey")
}

func createLine(level int, content string) string {
	if level == 0 {
		return content
	} else {
		spacer := "___"
		prefix := "|"

		for i := 0; i < (level - 1); i++ {
			prefix = prefix + " |"
		}
		line := prefix + spacer + content
		return line
	}
}

func downloadBlob(blobName string, containerUrl az.ContainerURL) bytes.Buffer {
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

	return downloadedData
}

func storeBlobContent(f *os.File, content string) {
	f.WriteString(fmt.Sprintf("%s\n", content))
}

func parseContainer(azContainer az.ContainerItem, p pipeline.Pipeline, accountName string, containerFilter string, blobFilter string, showContent bool, c chan *container, wg *sync.WaitGroup, marker az.Marker) {
	defer wg.Done()
	containerName := azContainer.Name

	// TODO substring match? to match containers: ['test-1', 'test-2'], term: 'test, matches ['test-1', 'test-2']
	if len(containerFilter) > 0 && !strings.Contains(containerName, containerFilter) {
		return
	}

	// new returns pointer
	containerResult := new(container)
	containerResult.Name = containerName

	containerURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))
	containerServiceURL := azblob.NewContainerURL(*containerURL, p)

	ctx := context.Background()

	//var output = createLine(0, containerServiceURL.String()+"\n")
	for blobMarker := (azblob.Marker{}); blobMarker.NotDone(); {
		listBlob, _ := containerServiceURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}})
		blobMarker = listBlob.NextMarker
		blobItems := listBlob.Segment.BlobItems
		containerResult.Blobs = parseBlobs(containerResult, blobItems, blobFilter, showContent, containerServiceURL)
	}

	c <- containerResult
}

func parseBlobs(containerResult *container, blobItems []az.BlobItemInternal, blobFilter string, showContent bool, containerURL azblob.ContainerURL) []blob {
	var blobWg sync.WaitGroup
	bc := make(chan blob)

	blobs := make([]blob, len(blobItems))
	for _, blobItem := range blobItems {
		if len(blobFilter) > 0 && !strings.Contains(blobItem.Name, blobFilter) {
			continue
		}
		blobWg.Add(1)
		go createBlobOutput(blobItem, &blobWg, bc, showContent, containerURL)
	}

	go func() {
		blobWg.Wait()
		close(bc)
	}()

	// channel to print
	for elem := range bc {
		blobs = append(blobs, elem)
	}
	return blobs
}

func parseBlobProperties(properties az.BlobProperties) map[string]string {
	result := make(map[string]string)
	//result["Blob Type"] = properties.BlobType
	result["Content MD5"] = b64.StdEncoding.EncodeToString(properties.ContentMD5)
	result["Created at"] = properties.CreationTime.String()
	result["Last modified at"] = properties.LastModified.String()
	//result["Lease Status"] = properties.LeaseStatus
	//result["Lease State"] = properties.LeaseState
	//result["Lease Duration"] = properties.LeaseDuration

	return result
}

func createBlobOutput(blobItem az.BlobItemInternal, wg *sync.WaitGroup, c chan blob, downloadContent bool, containerURL azblob.ContainerURL) {
	defer wg.Done()

	blob := new(blob)
	blob.Name = blobItem.Name
	blob.Properties = parseBlobProperties(blobItem.Properties)

	if downloadContent {
		blob.Content = downloadBlob(blobItem.Name, containerURL)
	}

	c <- *blob
}

func exec(args arguments) {
	var f *os.File
	var err error

	if args.StoreContent {
		outputFile := defaultFileName
		if len(args.FileName) > 0 {
			outputFile = args.FileName
		}

		f, err = os.Create(outputFile)
		if err != nil {
			log.Fatalf("Could not create file %s", outputFile)
		}
		defer f.Close()
	}

	ctx := context.Background()

	// Create a default request pipeline using your storage account name and account key
	credential, authErr := azblob.NewSharedKeyCredential(args.AccountName, args.AccessKey)
	if authErr != nil {
		log.Fatal("Error while Authentication")
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", args.AccountName))

	serviceURL := azblob.NewServiceURL(*URL, p)

	// TODO
	//line(!args.ContentOnly, 0, URL.String())

	c := make(chan *container)
	var wg sync.WaitGroup
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listContainer, err := serviceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})

		if err != nil {
			log.Fatal("Error while getting Container")
		}

		for _, val := range listContainer.ContainerItems {
			wg.Add(1)
			go parseContainer(val, p, args.AccountName, args.ContainerName, args.BlobName, args.ShowContent, c, &wg, marker)
		}
		// Pagination
		marker = listContainer.NextMarker
	}

	// wait for all entries in waitgroup an close channel
	go func() {
		wg.Wait()
		close(c)
	}()

	// channel to print
	for elem := range c {
		m, _ := json.Marshal(elem)
		fmt.Println(string(m))
		//fmt.Println(elem)
	}
}

// kudos to:
// https://github.com/Azure/azure-storage-blob-go/blob/456ab4777f89ceb54316ddf71d2acfd39bb86e1d/azblob/zt_examples_test.go
// and
// https://github.com/Azure-Samples/storage-blobs-go-quickstart/blob/master/storage-quickstart.go
func main() {
	start := time.Now()

	rootCmd.Execute()

	// Code to measure
	duration := time.Since(start)

	// Formatted string, such as "2h3m0.5s" or "4.503μs"
	fmt.Println(duration)
}
