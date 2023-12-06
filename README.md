# mastodon-flink-connector

Inspired by the retired [Flink Twitter connector](https://github.com/apache/flink/commit/4962689cecc57dc93b4298e35685d238a2472576#diff-a9a12d92e7b1aeb7c2ebde08f25687dfb9445538cf801c9da9b7737f4265a14e).

The [Mastodon Streaming API](https://docs.joinmastodon.org/methods/streaming/) provides access to the stream of toots made available by Mastodon.
This repository defines a `MastodonSource` class for establishing a connection to this stream.
To use this connector, add the following dependency to your project:

{{< artifact flink-connector-mastodon >}}

#### Authentication

In order to connect to the Mastodon stream the user has to register their program and acquire the necessary information for the authentication. The process is described below.

#### Acquiring the authentication information

First of all, a Mastodon account is needed. Sign up for free at [mastodon.social/auth/sign_up](https://mastodon.social/auth/sign_up)
and register the application by
clicking on `Settings > Development > New App` button. Fill out a form about your program and select the following permissions:

- `admin:read`

The Access Token (`mastodon-source.accessToken` in `MastodonSource`) can be generated and acquired by clicking on the app you just created.
Remember to keep this piece of information secret and do not push them to public repositories.

#### Usage

In contrast to other connectors, the `MastodonSource` depends on no additional services. For example the following code should run gracefully:

```java
Properties props = new Properties();
props.setProperty(MastodonSource.INSTANCE_STRING, "");
props.setProperty(MastodonSource.ACCESS_TOKEN, "");
DataStream<String> streamSource = env.addSource(new MastodonSource(props));
```

```scala
val props = new Properties()
props.setProperty(MastodonSource.INSTANCE_STRING, "")
props.setProperty(MastodonSource.ACCESS_TOKEN, "")
val streamSource = env.addSource(new MastodonSource(props))
```

The `MastodonSource` emits strings containing a JSON object, representing a Toot.

The `MastodonExample` class in the `flink-example-mastodon` package shows a full example how to use the `MastodonSource`.

By default, the `MastodonSource` uses the `StatusesSampleEndpoint`. This endpoint returns a random sample of Toots.
There is a `MastodonSource.EndpointInitializer` interface allowing users to provide a custom endpoint.
