import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: '⚡ Ultra Fast',
    description: (
      <>
        Designed for backend workloads. Uses <code>Span&lt;T&gt;</code> and SIMD 
        optimizations to process millions of rows in milliseconds.
      </>
    ),
  },
  {
    title: '🧠 Zero Allocation',
    description: (
      <>
        Built on top of <code>ArrayPool</code> and memory slicing. 
        LeichtFrame minimizes GC pressure to keep your application latency consistent.
      </>
    ),
  },
  {
    title: '🔗 Interoperable',
    description: (
      <>
        Native support for <strong>Apache Arrow</strong>, <strong>Parquet</strong>, and efficient CSV streaming. 
        Seamlessly integrates into modern data pipelines.
      </>
    ),
  },
];

function Feature({title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3" className={styles.featureTitle}>{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}