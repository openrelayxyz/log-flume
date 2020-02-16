/* eslint-disable */
import React from "react"
import Layout from "../components/Layout"
import Banner from "../components/Banner"
import StyledHero from "../components/StyledHero"
import Contact from "../components/Contact"
import styles from '../css/polish.module.css'
import { graphql } from "gatsby"
import AniLink from "gatsby-plugin-transition-link/AniLink"

export default ({ data }) => (
  <Layout>
      <Contact />
  </Layout>
)
export const query = graphql`
  query {
    defaultBcg: file(relativePath: { eq: "defaultBcg.jpeg" }) {
      childImageSharp {
        fluid(quality: 90, maxWidth: 4160) {
          ...GatsbyImageSharpFluid_withWebp
        }
      }
    }
  }
`
