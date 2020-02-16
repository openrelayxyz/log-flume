/* eslint-disable */
import React from "react"
import Navbar from "./Navbar"
import "./layout.css"

class Layout extends React.Component {
  render() {
    return (
      <main>
        <Navbar />
        {this.props.children}
      </main>
    )
  }
}

export default Layout
