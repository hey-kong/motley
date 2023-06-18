package ossutil

import (
	"io"
	"log"
	"os"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

func Download(bucket *oss.Bucket, srcObject string, path string) {
	err := bucket.GetObjectToFile(srcObject, path)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}

	log.Printf("%q has downloaded to %q\n", srcObject, path)
}

func Upload(bucket *oss.Bucket, path string, objectKey string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("Failed to open file %q, %v\n", path, err)
		return
	}
	defer f.Close()

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		log.Fatalf("Failed to reset seek to 0 for file %q, %v\n", path, err)
	}

	// Upload file to OSS
	err = bucket.PutObjectFromFile(objectKey, path)
	if err != nil {
		log.Fatalf("Failed to upload file %q, %v\n", path, err)
		return
	}
	log.Printf("Successfully uploaded %q\n", objectKey)
}

func Remove(bucket *oss.Bucket, objectKey string) {
	err := bucket.DeleteObject(objectKey)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}

	log.Printf("%q has removed\n", objectKey)
}

func Rename(bucket *oss.Bucket, srcObject string, destObject string) {
	// Copy srcobject to destobject in the same Bucket
	_, err := bucket.CopyObject(srcObject, destObject)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}

	// Remove srcobject
	err = bucket.DeleteObject(srcObject)
	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}

	log.Printf("%q has renamed %q\n", srcObject, destObject)
}
