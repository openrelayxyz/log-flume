// Implement the Gatsby API “onCreatePage”. This is
// called after every page is created.

exports.onCreateWebpackConfig = ({ stage, loaders, actions }) => {
  if (stage === "build-html") {
    actions.setWebpackConfig({
      externals: ['websocket', 'electron', 'pg-native', 'newrelic', 'web3', ],
      module: {
        rules: [
          {
            test: /react-stripe-elements/,
            use: loaders.null(),
          },
        ],
      },
    })
  }
}
