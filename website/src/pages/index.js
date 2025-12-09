import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import Heading from '@theme/Heading';
import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className={clsx('hero__title', styles.titleGradient)}>
          {siteConfig.title}
        </Heading>
        <p className={clsx('hero__subtitle', styles.subtitle)}>
          {siteConfig.tagline}
        </p>
        
        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/LeichtFrame/Core">
            LeichtFrame Core Documentation
          </Link>

          <Link
            className="button button--primary button--lg"
            to="/docs/LeichtFrame/IO">
            LeichtFrame IO Documentation
          </Link>
          
          <Link
            className="button button--secondary button--lg"
            to="https://github.com/dwalterskoetter/leichtframe">
            View on GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} | High-Performance .NET DataFrames`}
      description="Zero-allocation, high-performance DataFrame engine for .NET backend services.">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}