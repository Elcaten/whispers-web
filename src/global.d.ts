import * as React from 'react'

declare global {
  namespace JSX {
    interface IntrinsicElements {
      flex: React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
    }
  }
}