# telegram-fuse
A FUSE filesystem for Telegram storage, modified from [onedrive-fuse](https://github.com/oxalica/onedrive-fuse).

## Usage
```
telegram-fuse --app-id <your-telegram-app-id> --app-hash <your-telegram-app-hash> ~/telegram
```

### Parameters
|    Parameter    | Default | Function          |
| :-------------: | ------- | ----------------- |
|   `--app-id`    |         | telegram app id   |
|  `--app-hash`   |         | telegram app hash |
|   `--chat-id`   |         | telegram chat id  |
| `--async-flush` | `false` | async flush file  |
