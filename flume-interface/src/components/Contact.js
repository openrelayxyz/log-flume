/* eslint-disable */
import React from "react"
import styles from "../css/contact.module.css"
import TxHistory from "./TxHistory"

class Contact extends React.Component {
  constructor() {
    super();
    this.state = {}
  }
  trackAddress(e) {
    this.setState({_address: e.target.value})
  }
  setAddress(e) {
    console.log("Setting address")
    this.setState({address: this.state._address})
  }
  render() {
    return (
      <section className={styles.contact}>
        <div className={styles.center}>
          <div>
            <input
              type="text"
              name="ethaddress"
              id="ethaddress"
              className={styles.formControl}
              onChange={this.trackAddress.bind(this)}
              placeholder="Enter ETH address or .ens domain."
            />
          </div>
          <div className={styles.subbox}>
            <input
              type="button"
              value="Look Up"
              className={styles.submit}
              onClick={this.setAddress.bind(this)}
            />
          </div>
          <TxHistory address={this.state.address} />
        </div>
      </section>
    )
  }
}

export default Contact;
