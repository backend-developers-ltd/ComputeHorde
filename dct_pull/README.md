# DCT pull
Gets around the issue of watchtower not respecting DCT.

This is a small docker image that:
- uses passed-in docker socket
- wakes up once per minute
- with the DCT signature checks enabled, tries to pull in new image versions

That's it.

There is an assumption that there is also an instance of watchtower running - it will update
running containers to use the pulled images.