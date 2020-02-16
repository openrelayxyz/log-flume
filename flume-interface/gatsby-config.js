/**
 * Configure your Gatsby site with this file.
 *
 * See: https://www.gatsbyjs.org/docs/gatsby-config/
 */

module.exports = {

  siteMetadata: {
    title: "Flume",
    description: "The easy way to get all the logs.",
    author: "Rivet",
    data: ["item 1", "item 2"],
  },
  proxy: {
    prefix: "/api",
    url: "https://stage-mgmt-billing.stage.rivet.cloud",
    // url: "http://localhost:8001",
  },
  plugins: [
  `gatsby-transformer-sharp`,
  `gatsby-plugin-react-helmet`,
  `gatsby-plugin-sharp`,
  `gatsby-plugin-styled-components`,
  `gatsby-plugin-transition-link`,
  {
      resolve: `gatsby-plugin-stripe`,
      options: {
        async: true,
        crossOrigin: true,
      },
    },
    {
      resolve: `gatsby-source-filesystem`,
      options: {
        name: `images`,
        path: `${__dirname}/src/images/`,
      },
    },
  ],
}
