When one file of a `MultiUpload` fails to upload, the uploads still in progress are cancelled before `OutputUploadFailed` is raised; uploads that already finished stay delivered.
