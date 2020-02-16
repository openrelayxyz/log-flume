/* eslint-disable */
import React from "react"
import Layout from "../components/Layout"
import styles from "../css/error.module.css"
import Banner from "../components/Banner"
import AniLink from "gatsby-plugin-transition-link/AniLink"
import { Helmet } from "react-helmet"

export default function error() {
  return (
    <Layout>
      <Helmet>
        <title>Rivet | Not Found</title>
      </Helmet>
      <header className={styles.error}>
        <Banner title="oops it's a dead end">
          <AniLink fade to="/" className="btn-white">
            back to home page
          </AniLink>
        </Banner>
      </header>
    </Layout>
  )
}
