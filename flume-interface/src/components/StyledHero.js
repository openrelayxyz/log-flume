/* eslint-disable */
import React from "react"
import styled from "styled-components"
import BackgroundImage from "gatsby-background-image"
const StyledHero = ({ img, className, children, home }) => {
  return (
    <BackgroundImage className={className} fluid={img} home={home}>
      {children}
    </BackgroundImage>
  )
}

export default styled(StyledHero)`
  min-height: calc(100vh - 62px);
  background-position: center;
  background-size: cover;
  background-color:#000;
  display: flex;
  justify-content: center;
  align-items: center;
  text-shadow:0px 0px 6px #000;
`
